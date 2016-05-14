package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "github.com/go-sql-driver/mysql"
	"github.com/BurntSushi/toml"
	"errors"
	"time"
)

const (
	TASK_TYPE_DB_MYSQL_QUERY = 1
	TASK_TYPE_DB_MYSQL_EXEC = 2
)

type ConfigFile struct {
	Url   		string
	Interval	time.Duration
}

/**
A task from the API to be executed locally, then a JSON response returned
 */
type Task struct {
	RawConfig json.RawMessage	`json:"config"`
	Type      uint64		`json:"type"`
	Payload   string		`json:"payload"`
}

/**
Config for a DB task to initialise the DB connection
 */
type DBTaskConfig struct {
	Type		string		`json:"type"`
	Dsn 		string		`json:"dsn"`
}

/**
Used to map rows with unknown columns from a DB query so we can add them to a JSON response
 */
type MapStringScan struct {
	// cp are the column pointers
	cp 	[]interface{}
	// row contains the final result
	row      map[string]string
	colCount int
	colNames []string
}

/**
Initialise a mop for a row in the DB query result that will be updated with `rows.Scan()`
 */
func newMapStringScan(columnNames []string) *MapStringScan {
	lenCN := len(columnNames)
	s := &MapStringScan{
		cp:       make([]interface{}, lenCN),
		row:      make(map[string]string, lenCN),
		colCount: lenCN,
		colNames: columnNames,
	}
	for i := 0; i < lenCN; i++ {
		s.cp[i] = new(sql.RawBytes)
	}
	return s
}

/**
Update a row map from the db query result
 */
func (s *MapStringScan) Update(rows *sql.Rows) error {
	if err := rows.Scan(s.cp...); err != nil {
		return err
	}

	for i := 0; i < s.colCount; i++ {
		if rb, ok := s.cp[i].(*sql.RawBytes); ok {
			s.row[s.colNames[i]] = string(*rb)
			*rb = nil // reset pointer to discard current value to avoid a bug
		} else {
			return fmt.Errorf("Cannot convert index %d column %s to type *sql.RawBytes", i, s.colNames[i])
		}
	}
	return nil
}

/**
Get a map representing a row from DB query results
 */
func (s *MapStringScan) Get() map[string]string {
	return s.row
}

/**
Fetch a pending task from the API and populate a Task from the JSON response
 */
func getPendingTask(config ConfigFile) (Task, error) {

	var task Task

	resp, err := http.Get(config.Url); fck(err)
	rawResponse, err := ioutil.ReadAll(resp.Body); fck(err)

	//fmt.Println(string(rawResponse))

	if string(rawResponse) == "0" {
		return task, errors.New("No Tasks")
	}

	err = json.Unmarshal(rawResponse, &task); fck(err)

	return task, nil
}

/**
Get DB specific config to initialise a database connection
 */
func getDbTaskConfig(task Task) DBTaskConfig {
	var config DBTaskConfig
	err := json.Unmarshal(task.RawConfig, &config); fck(err)
	fmt.Print("Database Configuration: ")
	fmt.Println(config)

	return config
}

/**
Initialise database connection based on the task type
 */
func initDbConnection(task Task) *sql.DB {
	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		fmt.Println("Initilising Database Connection...")
		config := getDbTaskConfig(task)
		db, err := sql.Open(config.Type, config.Dsn); fck(err)
		return db
	default:
		panic("Task type not recognised")
	}
}

/**
POST the result of a task back to the API
 */
func postJsonResponse(config ConfigFile, data interface{}) {
	payload, err := json.Marshal(data); fck(err)

	resp, err := http.Post(config.Url, "application/json", bytes.NewBuffer(payload)); fck(err)

	contents, err := ioutil.ReadAll(resp.Body); fck(err)

	fmt.Println(string(contents))
}

/**
Is the current task a database query?
 */
func isDbTask (task Task) bool {
	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		return true
	default:
		return false
	}
}

/**
Open a DB connection, execute a query and POST the result back to the API
 */
func processDbTask(config ConfigFile, task Task) {

	db := initDbConnection(task)
	db.SetMaxIdleConns(100)
	defer db.Close()

	rows, err := db.Query(task.Payload); fck(err)

	columnNames, err := rows.Columns(); fck(err)

	var response []map[string]string

	rc := newMapStringScan(columnNames)
	for rows.Next() {
		err := rc.Update(rows); fck(err)
		cv := rc.Get()

		response = append(response, cv)
	}
	rows.Close()

	postJsonResponse(config, response)
}

func checkForTasks(config ConfigFile) bool {

	fmt.Println("Checking for tasks...")

	task, err := getPendingTask(config)

	if err != nil {
		fmt.Println(err)
		return false
	}

	if isDbTask(task) {
		processDbTask(config, task)
	}

	return true
}

/**
Handle an error
 */
func fck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/**
GO! (haha)
 */
func main() {

	var config ConfigFile
	_, err := toml.DecodeFile("config.toml", &config); fck(err)

	fmt.Print("Config:")
	fmt.Println(config)

	checkForTasks(config)

	// Create an interval timer to check for tasks every `config.Interval` seconds
	ticker := time.NewTicker(config.Interval * time.Second)
	for {
		select {
		case <-ticker.C:
		}
		checkForTasks(config)
	}

}
