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
)

const (
	API_URL = "http://taskserver:8888/"
	TASK_TYPE_DB_MYSQL_QUERY = 1
	TASK_TYPE_DB_MYSQL_EXEC = 2
)

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
	Type		string
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
func getPendingTask() Task {
	resp, err := http.Get(API_URL); fck(err)

	var task Task
	err = json.NewDecoder(resp.Body).Decode(&task); fck(err)

	return task
}

/**
Get DB specific config to initialise a database connection
 */
func getDbTaskConfig(task Task) DBTaskConfig {
	var config DBTaskConfig
	err := json.Unmarshal(task.RawConfig, &config); fck(err)

	return config
}

/**
Initialise database connection based on the task type
 */
func initDbConnection(task Task) *sql.DB {
	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		config := getDbTaskConfig(task)
		config.Type = "mysql"
		db, err := sql.Open(config.Type, config.Dsn); fck(err)
		return db
	default:
		panic("Task type not recognised")
	}
}

/**
POST the result of a task back to the API
 */
func postJsonResponse(data interface{}) {
	payload, err := json.Marshal(data); fck(err)

	resp, err := http.Post(API_URL, "application/json", bytes.NewBuffer(payload)); fck(err)

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
func processDbTask(task Task) {

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

	postJsonResponse(response)
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

	var task = getPendingTask()

	if isDbTask(task) {
		processDbTask(task)
	}
}
