package comparator

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/victorlunam/dbgo/internal/database"
	"github.com/victorlunam/dbgo/internal/models"
)

type Comparator struct {
	SourceDB         *database.Database
	TargetDB         *database.Database
	Results          []models.DiffResult
	ResultsMu        sync.Mutex
	IsLoggingEnabled bool
}

func NewComparator(sourceDB, targetDB *database.Database, isLoggingEnabled bool) *Comparator {
	return &Comparator{
		SourceDB:         sourceDB,
		TargetDB:         targetDB,
		Results:          []models.DiffResult{},
		IsLoggingEnabled: isLoggingEnabled,
	}
}

func (c *Comparator) Compare(objectTypes []string, timestamp string) error {
	if c.IsLoggingEnabled {
		logsDir := fmt.Sprintf("logs-%s-%s-%s", c.SourceDB.Config.Database, c.TargetDB.Config.Database, timestamp)
		if err := os.MkdirAll(logsDir, 0755); err != nil {
			return fmt.Errorf("error creating logs directory: %v", err)
		}
	}

	sourceObjects, err := c.SourceDB.GetObjectsList(objectTypes)
	if err != nil {
		return fmt.Errorf("error getting objects from source database: %v", err)
	}

	color.Cyan("Found %d objects to compare in the source database", len(sourceObjects))

	targetObjects, err := c.TargetDB.GetObjectsList(objectTypes)
	if err != nil {
		return fmt.Errorf("error getting objects from target database: %v", err)
	}

	targetObjectsMap := make(map[string]models.SchemaObject)
	for _, obj := range targetObjects {
		key := fmt.Sprintf("%s.%s.%s", obj.Schema, obj.Name, obj.Type)
		targetObjectsMap[key] = obj
	}

	fileName := fmt.Sprintf("schema-diff-%s-%s-%s-%s.sql", c.SourceDB.Config.Database, c.TargetDB.Config.Database, strings.Join(objectTypes, "-"), timestamp)
	outputFile, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outputFile.Close()

	outputFile.WriteString("-- Schema comparison script\n")
	outputFile.WriteString("-- Differences found between databases\n\n")

	// use WaitGroup for sync goroutines
	var wg sync.WaitGroup
	// limit the number of goroutines concurrent
	semaphore := make(chan struct{}, 10)

	// compare each object
	for _, sourceObj := range sourceObjects {
		wg.Add(1)

		go func(obj models.SchemaObject) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			key := fmt.Sprintf("%s.%s.%s", obj.Schema, obj.Name, obj.Type)
			targetObj, exists := targetObjectsMap[key]

			result := models.DiffResult{
				Object: obj,
				Exists: exists,
			}

			if !exists {
				color.Yellow("The object %s.%s (%s) does not exist in the target database", obj.Schema, obj.Name, obj.Type)

				sourceDefinition, err := c.SourceDB.GetObjectDefinition(obj)
				if err != nil {
					color.Red("Error getting definition of %s.%s: %v", obj.Schema, obj.Name, err)
					return
				}

				result.HasDifferences = true
				result.DifferenceScript = sourceDefinition
			} else {
				sourceDefinition, err := c.SourceDB.GetObjectDefinition(obj)
				if err != nil {
					color.Red("Error getting definition of source for %s.%s: %v", obj.Schema, obj.Name, err)
					return
				}

				targetDefinition, err := c.TargetDB.GetObjectDefinition(targetObj)
				if err != nil {
					color.Red("Error getting definition of target for %s.%s: %v", obj.Schema, obj.Name, err)
					return
				}

				normalizedSource := normalizeDefinition(sourceDefinition)
				normalizedTarget := normalizeDefinition(targetDefinition)

				if c.IsLoggingEnabled {
					logsDir := fmt.Sprintf("logs-%s-%s-%s", c.SourceDB.Config.Database, c.TargetDB.Config.Database, timestamp)
					sourceFileName := fmt.Sprintf("%s/SOURCE-%s-%s-%s.sql", logsDir, obj.Type, obj.Schema, obj.Name)
					if err := os.WriteFile(sourceFileName, []byte(normalizedSource), 0644); err != nil {
						color.Red("Error writing source definition file for %s.%s: %v", obj.Schema, obj.Name, err)
					}
				}

				if normalizedSource != normalizedTarget {
					color.Yellow("Differences found in %s.%s (%s)", obj.Schema, obj.Name, obj.Type)

					if c.IsLoggingEnabled {
						logsDir := fmt.Sprintf("logs-%s-%s-%s", c.SourceDB.Config.Database, c.TargetDB.Config.Database, timestamp)
						targetFileName := fmt.Sprintf("%s/TARGET-%s-%s-%s.sql", logsDir, obj.Type, obj.Schema, obj.Name)
						if err := os.WriteFile(targetFileName, []byte(normalizedTarget), 0644); err != nil {
							color.Red("Error writing target definition file for %s.%s: %v", obj.Schema, obj.Name, err)
						}
					}

					dropStatement, err := generateDropStatement(obj, c.SourceDB)
					if err != nil {
						color.Red("Error generating drop statement for %s.%s: %v", obj.Schema, obj.Name, err)
						return
					}

					result.HasDifferences = true
					result.DifferenceScript = fmt.Sprintf("%s\n%s", dropStatement, sourceDefinition)
				}
			}

			if result.HasDifferences {
				c.ResultsMu.Lock()
				c.Results = append(c.Results, result)
				c.ResultsMu.Unlock()
			}
		}(sourceObj)
	}

	wg.Wait()

	for _, result := range c.Results {
		outputFile.WriteString(fmt.Sprintf("-- Object: %s.%s (%s)\n", result.Object.Schema, result.Object.Name, result.Object.Type))
		outputFile.WriteString(result.DifferenceScript)
		outputFile.WriteString("GO\n\n")
	}

	color.Cyan("Found %d differences in %d objects", len(c.Results), len(sourceObjects))

	return nil
}

func normalizeDefinition(definition string) string {
	result := strings.ReplaceAll(definition, "\t", " ")

	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}

	result = strings.ReplaceAll(result, "\r\n", "\n")

	lines := strings.Split(result, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}

	linesToString := strings.Join(lines, "\n")

	return strings.TrimSpace(linesToString)
}

func generateDropStatement(obj models.SchemaObject, db *database.Database) (string, error) {
	dropStatement := ""
	var err error

	switch obj.Type {
	case "USER_TABLE":
		dropStatement, err = db.GetTableDropStatement(obj)
	case "VIEW":
		dropStatement = fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'V') IS NOT NULL DROP VIEW [%s].[%s];", obj.Schema, obj.Name, obj.Schema, obj.Name)
	case "SQL_STORED_PROCEDURE":
		dropStatement = fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'P') IS NOT NULL DROP PROCEDURE [%s].[%s];", obj.Schema, obj.Name, obj.Schema, obj.Name)
	case "SQL_SCALAR_FUNCTION", "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_TABLE_VALUED_FUNCTION":
		dropStatement = fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'FN') IS NOT NULL DROP FUNCTION [%s].[%s];", obj.Schema, obj.Name, obj.Schema, obj.Name)
	case "SQL_TRIGGER":
		dropStatement = fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'TR') IS NOT NULL DROP TRIGGER [%s].[%s];", obj.Schema, obj.Name, obj.Schema, obj.Name)
	default:
		dropStatement = fmt.Sprintf("-- Unknown object type: %s. It must be deleted manually.", obj.Type)
	}

	dropStatement = fmt.Sprintf("%s\nGO", dropStatement)

	return dropStatement, err
}
