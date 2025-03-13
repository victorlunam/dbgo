package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
	"github.com/victorlunam/dbgo/internal/comparator"
	"github.com/victorlunam/dbgo/internal/config"
	"github.com/victorlunam/dbgo/internal/database"
	"github.com/victorlunam/dbgo/internal/ui"
)

func main() {
	fmt.Println("=== Comparator DBGO ===")

	isLoggingEnabled := checkLoggingEnabled(&os.Args)

	source := readDatabaseConfig("SOURCE DATABASE")

	sourceDB, err := database.Connect(source)
	if err != nil {
		color.Red("Error connecting to source database: %v", err)
		os.Exit(1)
	}
	color.Green("Successfully connected to source database")
	defer sourceDB.Close()

	target := readDatabaseConfig("TARGET DATABASE")

	targetDB, err := database.Connect(target)
	if err != nil {
		color.Red("Error connecting to target database: %v", err)
		os.Exit(1)
	}
	color.Green("Successfully connected to target database")
	defer targetDB.Close()

	objectTypes := selectObjectTypes()

	comp := comparator.NewComparator(sourceDB, targetDB, isLoggingEnabled)
	timestamp := time.Now().Format("20060102150405")

	err = comp.Compare(objectTypes, timestamp)
	if err != nil {
		color.Red("Error during comparison: %v", err)
		os.Exit(1)
	}

	fileName := fmt.Sprintf("schema-diff-%s-%s-%s-%s.sql", source.Database, target.Database, strings.Join(objectTypes, "-"), timestamp)
	color.Green("Comparison completed. The results are in the '%s' file", fileName)
}

func checkLoggingEnabled(args *[]string) bool {
	isLoggingEnabled := false
	for i, arg := range *args {
		if arg == "--log" || arg == "-l" {
			isLoggingEnabled = true
			*args = append((*args)[:i], (*args)[i+1:]...)
			break
		}
	}

	if isLoggingEnabled {
		color.Green("Logging enabled")
	}

	return isLoggingEnabled
}

func readDatabaseConfig(dbLabel string) config.DatabaseConfig {
	// Try to read from config file first
	if configFile, err := os.ReadFile("dbgo.config.json"); err == nil {
		var config struct {
			Source config.DatabaseConfig `json:"source"`
			Target config.DatabaseConfig `json:"target"`
		}
		if err := json.Unmarshal(configFile, &config); err == nil {
			if dbLabel == "SOURCE DATABASE" {
				return config.Source
			}
			return config.Target
		}
	}

	var dbConfig config.DatabaseConfig

	color.Cyan("Configuration for %s:", dbLabel)

	fmt.Print("Server (default: localhost): ")
	fmt.Scanln(&dbConfig.Server)
	if dbConfig.Server == "" {
		dbConfig.Server = "localhost"
	}

	fmt.Print("Port (default: 1433): ")
	fmt.Scanln(&dbConfig.Port)
	if dbConfig.Port == "" {
		dbConfig.Port = "1433"
	}

	fmt.Print("User: ")
	fmt.Scanln(&dbConfig.User)

	fmt.Print("Password: ")
	fmt.Scanln(&dbConfig.Password)

	fmt.Print("Database: ")
	fmt.Scanln(&dbConfig.Database)
	if dbConfig.Database == "" {
		log.Fatal("The database name is required")
	}

	return dbConfig
}

func selectObjectTypes() []string {
	objectTypes := []string{"TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER"}

	selector := ui.NewSelectorModel(objectTypes)

	program := tea.NewProgram(selector)
	m, err := program.Run()
	if err != nil {
		color.Red("Error running program: %v", err)
		os.Exit(1)
	}

	// Type assert the final model to access the selected options
	finalModel := m.(ui.Model)

	selectedItems := []string{}
	for _, item := range finalModel.List.Items() {
		if i, ok := item.(ui.Item); ok && i.Selected {
			selectedItems = append(selectedItems, i.Value)
		}
	}
	return selectedItems
}
