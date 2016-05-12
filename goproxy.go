package main

import (
	"encoding/json"
	"database/sql"
	"net/http"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"bytes"
	"io/ioutil"
)

const (
	API_URL = "http://taskserver:8888/"
	TASK_TYPE_DB_MYSQL_QUERY = 1
	TASK_TYPE_DB_MYSQL_EXEC = 2
)

type MysqlTaskConfig struct {
	Type		string
	Dsn 		string		`json:"dsn"`
}
type Task struct {
	RawConfig	json.RawMessage	`json:"config"`
	Type		uint64		`json:"type"`
	Payload		string		`json:"payload"`

	Config    	MysqlTaskConfig
}

/**
  using a map
*/
type mapStringScan struct {
	// cp are the column pointers
	cp 	[]interface{}
	// row contains the final result
	row      map[string]string
	colCount int
	colNames []string
}

func NewMapStringScan(columnNames []string) *mapStringScan {
	lenCN := len(columnNames)
	s := &mapStringScan{
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

func (s *mapStringScan) Update(rows *sql.Rows) error {
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

func (s *mapStringScan) Get() map[string]string {
	return s.row
}

func getTask () Task {
	resp, err := http.Get(API_URL)
	if err != nil {
		panic(err)
	}

	var task Task
	err = json.NewDecoder(resp.Body).Decode(&task)
	if err != nil {
		panic(err)
	}

	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		var config MysqlTaskConfig
		err = json.Unmarshal(task.RawConfig, &config)
		if err != nil {
			panic(err)
		}
		config.Type = "mysql"
		task.Config = config
	}

	return task
}

func isDbTask (task Task) bool {
	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		return true
	default:
		return false
	}
}

func postJsonResponse(data interface{}) {
	payload, err := json.Marshal(data)
	fck(err)

	//fmt.Println(string(payload))

	resp, err := http.Post(API_URL, "application/json", bytes.NewBuffer(payload))
	fck(err)

	contents, err := ioutil.ReadAll(resp.Body)
	fck(err)

	fmt.Println(string(contents))
}

func fck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	var task = getTask()

	if isDbTask(task) {
		db, err := sql.Open(task.Config.Type, task.Config.Dsn)
		fck(err)

		db.SetMaxIdleConns(100)
		defer db.Close()

		rows, err := db.Query(task.Payload)
		fck(err)

		columnNames, err := rows.Columns()
		fck(err)

		var response []map[string]string

		rc := NewMapStringScan(columnNames)
		for rows.Next() {
			err := rc.Update(rows)
			fck(err)
			cv := rc.Get()

			response = append(response, cv)
		}
		rows.Close()

		postJsonResponse(response)
	}
}
