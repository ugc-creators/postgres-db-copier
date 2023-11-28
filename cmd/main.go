package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	transformations "github.com/ChanningDefoe/db_copy/internal/transformers"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

type Config struct {
	FromDatabase DatabaseConfig `yaml:"from_database"`
	ToDatabase   DatabaseConfig `yaml:"to_database"`
	Tables       []TableConfig  `yaml:"tables"`
}

type DatabaseConfig struct {
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
}

type TableConfig struct {
	Name             string                     `yaml:"name"`
	ColumnTransforms map[string]ColumnTransform `yaml:"column_transforms"`
}

type ColumnTransform struct {
	Type string `yaml:"type"`
}

func main() {
	config := readConfig("config.yml")

	fromDB := connectToDB(config.FromDatabase)
	toDB := connectToDB(config.ToDatabase)
	defer fromDB.Close()
	defer toDB.Close()

	for _, table := range config.Tables {
		copyTable(fromDB, toDB, table)
	}
}

func readConfig(filename string) Config {
	var config Config

	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("error reading YAML file: %v", err)
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("error parsing YAML file: %v", err)
	}

	return config
}

func connectToDB(dbConfig DatabaseConfig) *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, dbConfig.Name)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}

	return db
}

func copyTable(fromDB, toDB *sql.DB, tableConfig TableConfig) {
	query := fmt.Sprintf("SELECT * FROM %s", tableConfig.Name)

	rows, err := fromDB.Query(query)
	if err != nil {
		log.Printf("error querying from table %s: %v", tableConfig.Name, err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Printf("error getting columns for table %s: %v", tableConfig.Name, err)
		return
	}

	transformers := createTransformers(tableConfig.ColumnTransforms)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		// Apply transformations
		for i, col := range columns {
			if transformer, ok := transformers[col]; ok {
				values[i] = transformer.Transform(values[i])
			}
		}

		insertStmt := generateInsertStatement(tableConfig.Name, columns, values)
		_, err := toDB.Exec(insertStmt)
		if err != nil {
			log.Printf("error inserting into table %s: %v", tableConfig.Name, err)
			return
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("error iterating rows for table %s: %v", tableConfig.Name, err)
	}
}

func createTransformers(transformConfigs map[string]ColumnTransform) map[string]transformations.Transformer {
	transformers := make(map[string]transformations.Transformer)
	for col, config := range transformConfigs {
		fmt.Printf("Creating transformer for column %s has type %s\n", col, config.Type)
		switch config.Type {
		case "email":
			transformers[col] = transformations.EmailTransformer{}
		}
	}
	return transformers
}

func generateInsertStatement(tableName string, columns []string, values []interface{}) string {
	var cols, vals []string
	for i, col := range columns {
		var valStr string
		switch v := values[i].(type) {
		case time.Time:
			// Format time in postgres tz format and enclose in single quotes
			valStr = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999-07:00"))
		default:
			// Handle JSON types
			jsonVal, err := json.Marshal(v)
			if err == nil {
				valStr = fmt.Sprintf("'%s'", string(jsonVal))
			} else {
				valStr = fmt.Sprintf("'%v'", v)
			}
			if strVal, ok := v.(string); ok {
				valStr = fmt.Sprintf("'%s'", strings.ReplaceAll(strVal, "'", "''"))
			}
		}
		cols = append(cols, col)
		vals = append(vals, valStr)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(cols, ", "), strings.Join(vals, ", "))
}
