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
	"github.com/google/uuid"
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
	// Quote the table name to handle reserved keywords
	quotedTableName := fmt.Sprintf("\"%s\"", tableConfig.Name)
	query := fmt.Sprintf("SELECT * FROM %s", quotedTableName)

	// Truncate the destination table before copying
	truncateStmt := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", quotedTableName)
	if _, err := toDB.Exec(truncateStmt); err != nil {
		log.Fatalf("error truncating table %s: %v", tableConfig.Name, err)
		return
	}

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
			fmt.Println(insertStmt)
			log.Printf("error inserting into table %s: %v", tableConfig.Name, err)
			panic(err)
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
		case "nil":
			transformers[col] = transformations.NilTransformer{}
		case "random_email":
			transformers[col] = transformations.RandomEmailTransformer{}
		case "empty_json":
			transformers[col] = transformations.EmptyJSONTransformer{}
		}
	}
	return transformers
}

func generateInsertStatement(tableName string, columns []string, values []interface{}) string {
	var cols, vals []string
	for i, col := range columns {
		var valStr string
		switch v := values[i].(type) {
		case nil:
			valStr = "NULL"
		case time.Time:
			// Format time in postgres tz format and enclose in single quotes
			valStr = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999-07:00"))
		// case string:
		// 	if _, err := uuid.Parse(v); err == nil {
		// 		valStr = fmt.Sprintf("'%s'", v)
		// 	} else {
		// 		valStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		// 	}
		case []byte:
			if len(v) == 16 {
				valStr = fmt.Sprintf("'%s'", uuid.UUID(v).String())
			} else {
				// Attempt to unmarshal as JSON
				var jsonObj interface{}
				err := json.Unmarshal(v, &jsonObj)
				if err == nil {
					// Successfully unmarshaled JSON, now check if it's an array
					if jsonArr, isArray := jsonObj.([]interface{}); isArray {
						// Handle JSON array
						jsonVals := make([]string, len(jsonArr))
						for i, jsonVal := range jsonArr {
							jsonValStr, _ := json.Marshal(jsonVal)
							jsonVals[i] = string(jsonValStr)
						}
						valStr = fmt.Sprintf("'{%s}'", strings.Join(jsonVals, ","))
					} else {
						// It's a regular JSON object, handle it as a string
						// valStr = fmt.Sprintf("'%s'", string(v))
						// fix for single quotes in json
						valStr = fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
					}
				} else {
					// Not JSON, handle as a string
					valStr = fmt.Sprintf("'%s'", string(v))
				}
			}
		default:
			// print type
			// fmt.Printf("Type: %T\n", v)
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
		// Quote column names to handle reserved keywords
		quotedCol := fmt.Sprintf("\"%s\"", col)
		cols = append(cols, quotedCol)
		vals = append(vals, valStr)
	}
	// return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(cols, ", "), strings.Join(vals, ", "))
	// Quote table name to handle reserved keywords
	quotedTableName := fmt.Sprintf("\"%s\"", tableName)
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quotedTableName, strings.Join(cols, ", "), strings.Join(vals, ", "))
}
