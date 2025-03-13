package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/victorlunam/dbgo/internal/config"
	"github.com/victorlunam/dbgo/internal/models"
)

type Database struct {
	Config config.DatabaseConfig
	DB     *sql.DB
}

func Connect(config config.DatabaseConfig) (*Database, error) {
	query := url.Values{}
	query.Add("app name", "DBGO")

	connString := fmt.Sprintf("Server=%s,%s;Database=%s;User Id=%s;Password=%s;TrustServerCertificate=true",
		config.Server, config.Port, config.Database, config.User, config.Password)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &Database{
		Config: config,
		DB:     db,
	}, nil
}

func (d *Database) Close() error {
	return d.DB.Close()
}

func (d *Database) GetObjectsList(objectTypes []string) ([]models.SchemaObject, error) {
	var objects []models.SchemaObject
	ctx := context.Background()

	query := `
	SELECT 
		SCHEMA_NAME(o.schema_id) as schema_name,
		o.name as object_name, 
		o.type_desc as object_type 
	FROM 
		sys.objects o
	WHERE 
		o.type_desc IN (%s)
		AND o.is_ms_shipped = 0
	ORDER BY 
		o.type_desc, o.name
	`

	typeMapping := map[string]string{
		"TABLE":     "'USER_TABLE'",
		"VIEW":      "'VIEW'",
		"PROCEDURE": "'SQL_STORED_PROCEDURE'",
		"FUNCTION":  "'SQL_SCALAR_FUNCTION', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_TABLE_VALUED_FUNCTION'",
		"TRIGGER":   "'SQL_TRIGGER'",
	}

	var sqlTypes []string
	for _, objType := range objectTypes {
		if mappedType, ok := typeMapping[objType]; ok {
			sqlTypes = append(sqlTypes, mappedType)
		}
	}

	if len(objectTypes) > 0 {
		typesStr := strings.Join(sqlTypes, ", ")
		finalQuery := fmt.Sprintf(query, typesStr)

		rows, err := d.DB.QueryContext(ctx, finalQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var obj models.SchemaObject
			if err := rows.Scan(&obj.Schema, &obj.Name, &obj.Type); err != nil {
				return nil, err
			}
			objects = append(objects, obj)
		}
	}

	return objects, nil
}

func (d *Database) GetObjectDefinition(obj models.SchemaObject) (string, error) {
	ctx := context.Background()
	var definition string

	switch obj.Type {
	case "USER_TABLE": // for tables, get the definition through a query to sys.columns
		tableQuery := `
		WITH IndexCTE AS (
			SELECT 
				ic.object_id,
				ic.index_id,
				i.name AS index_name,
				i.type_desc AS index_type,
				i.is_primary_key,
				i.is_unique,
				i.is_unique_constraint,
				(
					SELECT c.name + ',' 
					FROM sys.index_columns ic2
					JOIN sys.columns c ON ic2.object_id = c.object_id AND ic2.column_id = c.column_id
					WHERE ic2.object_id = ic.object_id AND ic2.index_id = ic.index_id
					ORDER BY ic2.key_ordinal
					FOR XML PATH('')
				) AS columns
			FROM 
				sys.indexes i
			JOIN 
				sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			WHERE 
				i.name IS NOT NULL
			GROUP BY 
				ic.object_id, ic.index_id, i.name, i.type_desc, i.is_primary_key, i.is_unique, i.is_unique_constraint
		)
		SELECT
			'CREATE TABLE [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '] (' + CHAR(10) +
			(
				SELECT 
					'    [' + c.name + '] ' + 
					CASE 
						WHEN c.is_computed = 1 THEN 'AS ' + cc.definition 
						ELSE 
							'[' + tp.name + ']' + 
							CASE 
								WHEN tp.name IN ('varchar', 'nvarchar', 'char', 'nchar') THEN '(' + 
									CASE WHEN c.max_length = -1 THEN 'MAX' 
									ELSE 
										CASE WHEN tp.name IN ('nvarchar', 'nchar') 
											THEN CAST(c.max_length/2 AS VARCHAR(10)) 
											ELSE CAST(c.max_length AS VARCHAR(10)) 
										END 
									END + ')'
								WHEN tp.name IN ('decimal', 'numeric') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ', ' + CAST(c.scale AS VARCHAR(10)) + ')'
								ELSE ''
							END +
							-- Add IDENTITY property if column is identity
							CASE WHEN c.is_identity = 1 
								THEN ' IDENTITY(' + 
									CAST(IDENT_SEED(SCHEMA_NAME(t.schema_id) + '.' + t.name) AS VARCHAR(10)) + ',' + 
									CAST(IDENT_INCR(SCHEMA_NAME(t.schema_id) + '.' + t.name) AS VARCHAR(10)) + ')'
								ELSE '' 
							END +
							CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
					END +
					CASE WHEN c.column_id = (SELECT MAX(column_id) FROM sys.columns c2 WHERE c2.object_id = t.object_id) AND 
						NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.is_primary_key = 1)
						THEN ''
						ELSE ','
					END + CHAR(10)
				FROM 
					sys.columns c
				LEFT JOIN 
					sys.types tp ON c.user_type_id = tp.user_type_id
				LEFT JOIN 
					sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
				WHERE 
					c.object_id = t.object_id
				ORDER BY 
					c.column_id
				FOR XML PATH('')
			) +
			ISNULL((
				SELECT 
					CASE 
						WHEN i.is_primary_key = 1 THEN '    CONSTRAINT [' + i.index_name + '] PRIMARY KEY ' + 
							CASE WHEN i.index_type LIKE '%CLUSTER%' THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END +
							' (' + ISNULL(STUFF(i.columns, LEN(i.columns), 1, ''), '') + ')' + CHAR(10)
						WHEN i.is_unique_constraint = 1 THEN '    CONSTRAINT [' + i.index_name + '] UNIQUE ' + 
							CASE WHEN i.index_type LIKE '%CLUSTER%' THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END +
							' (' + ISNULL(STUFF(i.columns, LEN(i.columns), 1, ''), '') + ')' + CHAR(10)
						ELSE ''
					END
				FROM 
					IndexCTE i
				WHERE 
					i.object_id = t.object_id
				AND
					(i.is_primary_key = 1 OR i.is_unique_constraint = 1)
				FOR XML PATH('')
			), '') +
			');' + CHAR(10) + 'GO' + CHAR(10) + CHAR(10) +
			-- 'SET ANSI_PADDING OFF' + CHAR(10) + 'GO' + CHAR(10) + CHAR(10) +
			-- Generate default constraints as separate ALTER TABLE statements
			ISNULL((
				SELECT 
					'ALTER TABLE [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '] ADD CONSTRAINT [' + 
					dc.name + '] DEFAULT ' + dc.definition + ' FOR [' + c.name + '];' + CHAR(10) + 'GO' + CHAR(10) + CHAR(10)
				FROM 
					sys.columns c
				JOIN 
					sys.default_constraints dc ON c.default_object_id = dc.object_id
				WHERE 
					c.object_id = t.object_id
				FOR XML PATH('')
			), '')
		FROM 
			sys.tables t
		WHERE 
			t.name = @name AND SCHEMA_NAME(t.schema_id) = @schema
		`

		err := d.DB.QueryRowContext(ctx, tableQuery, sql.Named("name", obj.Name), sql.Named("schema", obj.Schema)).Scan(&definition)
		if err != nil {
			return "", err
		}

		// get foreign key constraints separately
		fkQuery := `
		SELECT 
			'ALTER TABLE [' + SCHEMA_NAME(tab.schema_id) + '].[' + tab.name + ']  WITH CHECK ADD  CONSTRAINT [' + 
			fk.name + '] FOREIGN KEY([' + 
			ISNULL(STUFF((
				SELECT ',' + COL_NAME(fkc.parent_object_id, fkc.parent_column_id)
				FROM sys.foreign_key_columns fkc
				WHERE fkc.constraint_object_id = fk.object_id
				ORDER BY fkc.constraint_column_id
				FOR XML PATH('')
			), 1, 1, ''), '') + '])' + 
			CHAR(10) + 'REFERENCES [' + SCHEMA_NAME(ref_tab.schema_id) + '].[' + ref_tab.name + '] ([' +
			ISNULL(STUFF((
				SELECT ',' + COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id)
				FROM sys.foreign_key_columns fkc
				WHERE fkc.constraint_object_id = fk.object_id
				ORDER BY fkc.constraint_column_id
				FOR XML PATH('')
			), 1, 1, ''), '') + '])' + CHAR(10) + 'GO' + CHAR(10) + CHAR(10) +
			'ALTER TABLE [' + SCHEMA_NAME(tab.schema_id) + '].[' + tab.name + '] CHECK CONSTRAINT [' + 
			fk.name + ']' + CHAR(10)
		FROM 
			sys.foreign_keys fk
		JOIN 
			sys.tables tab ON fk.parent_object_id = tab.object_id
		JOIN 
			sys.tables ref_tab ON fk.referenced_object_id = ref_tab.object_id
		WHERE 
			tab.name = @name AND SCHEMA_NAME(tab.schema_id) = @schema
		ORDER BY fk.name;
		`

		fkRows, err := d.DB.QueryContext(ctx, fkQuery, sql.Named("name", obj.Name), sql.Named("schema", obj.Schema))
		if err != nil {
			return "", err
		}
		defer fkRows.Close()

		var fkConstraints []string
		for fkRows.Next() {
			var fkStatement string
			if err := fkRows.Scan(&fkStatement); err != nil {
				return "", err
			}
			fkConstraints = append(fkConstraints, fkStatement)
		}

		if len(fkConstraints) > 0 {
			definition += "\n" + strings.Join(fkConstraints, "\n")
		}

	default: // for views, procedures, functions and triggers, use sys.sql_modules
		query := `
		SELECT definition
		FROM sys.sql_modules m
		JOIN sys.objects o ON m.object_id = o.object_id
		WHERE o.name = @name AND SCHEMA_NAME(o.schema_id) = @schema
		`

		err := d.DB.QueryRowContext(ctx, query, sql.Named("name", obj.Name), sql.Named("schema", obj.Schema)).Scan(&definition)
		if err != nil {
			return "", err
		}
	}

	return definition, nil
}

func (d *Database) GetTableDropStatement(obj models.SchemaObject) (string, error) {
	ctx := context.Background()
	var dropStatement string

	query := `
	SELECT 
		ISNULL(STUFF((
			-- Get Foreign Key constraint drops
			SELECT CHAR(10) + 'ALTER TABLE [' + SCHEMA_NAME(tab.schema_id) + '].[' + tab.name + '] DROP CONSTRAINT [' + fk.name + ']' + CHAR(10) + 'GO' + CHAR(10)
			FROM sys.foreign_keys fk
			JOIN sys.tables tab ON fk.parent_object_id = tab.object_id
			WHERE tab.name = @name AND SCHEMA_NAME(tab.schema_id) = @schema
			FOR XML PATH('')
		), 1, 1, ''), '') +
		ISNULL(STUFF((
			-- Get Default constraint drops
			SELECT CHAR(10) + 'ALTER TABLE [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '] DROP CONSTRAINT [' + dc.name + ']' + CHAR(10) + 'GO' + CHAR(10)
			FROM sys.tables t
			JOIN sys.default_constraints dc ON t.object_id = dc.parent_object_id
			WHERE t.name = @name AND SCHEMA_NAME(t.schema_id) = @schema
			FOR XML PATH('')
		), 1, 1, ''), '') +
		-- Add final table drop
		CHAR(10) + 'IF OBJECT_ID(''[' + @schema + '].[' + @name + ']'', ''U'') IS NOT NULL' + CHAR(10) +
		'DROP TABLE [' + @schema + '].[' + @name + ']' + CHAR(10)
	`

	err := d.DB.QueryRowContext(ctx, query,
		sql.Named("name", obj.Name),
		sql.Named("schema", obj.Schema)).Scan(&dropStatement)
	if err != nil {
		return "", err
	}

	return dropStatement, nil
}

func containsObjectType(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
