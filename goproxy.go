package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kardianos/service"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"
)

const (
	TASK_TYPE_DB_MYSQL_QUERY = 1
	TASK_TYPE_DB_MYSQL_EXEC  = 2
	API_URL                  = "http://taskserver:8888/"
	INTERVAL                 = 10
)

var (
	svcFlag   string
	svcLogger service.Logger
	config    ConfigFile
	quit      chan bool
)

type Program struct {
	Exit    chan struct{}
	Service service.Service
	Cmd     *exec.Cmd
}

type ConfigFile struct {
	Url      string `json:"url"`
	Interval int    `json:"interval"`
	ApiKey   string `json:"key"`
}

/**
A task from the API to be executed locally, then a JSON response returned
*/
type Task struct {
	Id        string          `json:"id"`
	RawConfig json.RawMessage `json:"config"`
	Type      uint64          `json:"type"`
	Payload   string          `json:"payload"`
}

/**
Config for a DB task to initialise the DB connection
*/
type DBTaskConfig struct {
	Type string `json:"type"`
	Dsn  string `json:"dsn"`
}

/**
Used to map rows with unknown columns from a DB query so we can add them to a JSON response
*/
type MapStringScan struct {
	// cp are the column pointers
	cp []interface{}
	// row contains the final result
	row      map[string]string
	colCount int
	colNames []string
}

/**
Used to return responses to the task server e.g. `{"type": "error", "body": "Invalid API Key."}`
*/
type JsonResponse struct {
	Type string      `json:"type"`
	Body interface{} `json:"body"`
}

func (p *Program) Start(s service.Service) error {
	svcLogger.Info("Starting...")
	// Start should not block. Do the actual work async.
	go p.run()
	return nil
}
func (p *Program) run() {

	svcLogger.Info("Running...")
	// Check for tasks immediately
	checkForTasks()

	parsedInterval, err := time.ParseDuration(fmt.Sprintf("%ds", config.Interval))
	errCheck(err)

	// Create an interval timer to check for tasks every `config.Interval` seconds
	ticker := time.NewTicker(parsedInterval)
	for {
		select {
		case <-ticker.C:
		}
		checkForTasks()
	}
}
func (p *Program) Stop(s service.Service) error {
	svcLogger.Info("Stopping...")
	// Stop should not block. Return with a few seconds.
	return nil
}

/**
Validate the config object - it must have an API key
*/
func (c *ConfigFile) Validate() error {

	if "" == c.ApiKey {
		return errors.New("Invalid API Key.")
	}

	return nil
}

/**
Read in configuration from a JSON config file - this can be overridden by command line arguments.
If any config is overridden, the `config.json` file is updated.
*/
func loadConfiguration() {
	apiKey := flag.String("key", "", "Digistorm API Key.")
	apiUrl := flag.String("url", API_URL, "Digistorm API Key.")
	interval := flag.Int("interval", INTERVAL, "Digistorm API Key.")
	flag.StringVar(&svcFlag, "service", "", "Control the system service.")

	flag.Parse()

	_, filename, _, _ := runtime.Caller(1)
	configFilePath := path.Join(path.Dir(filename), "conf.json")

	file, err := os.Open(configFilePath)
	if errCheckFatal(err) == true {
		return
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if errCheckFatal(err) == true {
		return
	}

	var configChanged bool = false

	if config.Url == "" {
		config.Url = *apiUrl
		configChanged = true
	}
	if config.Interval == 0 {
		config.Interval = *interval
		configChanged = true
	}
	if config.ApiKey == "" {
		config.ApiKey = *apiKey
		configChanged = true
	}

	if configChanged == true {
		configData, err := json.Marshal(config)
		errCheckFatal(err)
		err = ioutil.WriteFile(configFilePath, configData, 0644)
		errCheckFatal(err)
	}

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
func getPendingTask() (Task, error) {

	var task Task

	req, err := http.NewRequest("GET", config.Url, nil)
	errCheckPostback(err)

	req.Header.Set("X-Digistorm-Key", config.ApiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	errCheckPostback(err)

	rawResponse, err := ioutil.ReadAll(resp.Body)
	errCheckPostback(err)

	if string(rawResponse) == "0" {
		return task, errors.New("No Tasks")
	}

	err = json.Unmarshal(rawResponse, &task)
	errCheckPostback(err)

	fmt.Print("Task found: ")
	fmt.Println(task.Id)

	return task, nil
}

/**
Get DB specific config to initialise a database connection
*/
func getDbTaskConfig(task Task) DBTaskConfig {
	var dbConfig DBTaskConfig
	err := json.Unmarshal(task.RawConfig, &dbConfig)
	errCheckPostback(err)
	fmt.Print("Database Configuration: ")
	fmt.Println(dbConfig)

	return dbConfig
}

/**
Initialise database connection based on the task type
*/
func initDbConnection(task Task) *sql.DB {
	switch task.Type {
	case TASK_TYPE_DB_MYSQL_QUERY, TASK_TYPE_DB_MYSQL_EXEC:
		fmt.Println("Initilising Database Connection...")
		config := getDbTaskConfig(task)
		db, err := sql.Open(config.Type, config.Dsn)
		errCheckPostback(err)
		return db
	default:
		panic("Task type not recognised")
	}
}

/**
POST the result of a task back to the API
*/
func postJsonResponse(response JsonResponse) {
	payload, err := json.Marshal(response)
	errCheck(err)

	req, err := http.NewRequest("POST", config.Url, bytes.NewBuffer(payload))
	errCheck(err)

	req.Header.Set("X-Digistorm-Key", config.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	errCheck(err)

	contents, err := ioutil.ReadAll(resp.Body)
	errCheck(err)

	fmt.Println(string(contents))
}

/**
Is the current task a database query?
*/
func isDbTask(task Task) bool {
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

	rows, err := db.Query(task.Payload)
	errCheckPostback(err)

	columnNames, err := rows.Columns()
	errCheckPostback(err)

	var response []map[string]string

	rc := newMapStringScan(columnNames)
	for rows.Next() {
		err := rc.Update(rows)
		errCheckPostback(err)
		cv := rc.Get()

		response = append(response, cv)
	}
	rows.Close()

	postJsonResponse(JsonResponse{
		Type: "success",
		Body: response,
	})
}

/**
Query the task server to see if it returns a task.
If a task is returned, process it
*/
func checkForTasks() {

	// Create a channel to execute this iteration of task fetching - can be closed on error without killing the exe
	quit = make(chan bool)

	go func() {
		fmt.Println("Checking for tasks...")

		task, err := getPendingTask()
		if err != nil {
			fmt.Println(err)
			return
		}

		if isDbTask(task) {
			processDbTask(task)
		}
	}()

}

/**
Handle an error - returns true if error was handled
*/
func errCheck(err error) bool {
	if err != nil {
		fmt.Println(err)

		// Close the currently running channel
		quit <- true

		return true
	}

	return false
}

/**
Handle an error - returns true if error was handled
*/
func errCheckFatal(err error) {
	if err != nil {

		// Close the currently running channel
		quit <- true

		log.Fatal(err)
	}
}

/**
Handle an error - returns true if error was handled
*/
func errCheckPostback(err error) bool {
	if err != nil {
		fmt.Println(err)

		// POST the error back to the task server
		postJsonResponse(JsonResponse{
			Type: "error",
			Body: err,
		})

		// Close the currently running channel
		quit <- true

		return true
	}

	return false
}

/**
GO! (haha)
*/
func main() {

	loadConfiguration()

	err := config.Validate()
	if err != nil {
		errCheckFatal(err)
	}

	svcConfig := &service.Config{
		Name:        "DigistormConnector",
		DisplayName: "Digistorm Connector",
		Description: "Runs as a service querying the Digistorm API for tasks to perform on the local machine e.g. executing a database query and then POSTing the result back to the Digistorm API.",
	}

	program := &Program{}

	s, err := service.New(program, svcConfig)
	if err != nil {
		errCheckFatal(err)
	}

	svcLogger, err = s.Logger(nil)
	if err != nil {
		errCheckFatal(err)
	}

	if len(svcFlag) != 0 {

		err := service.Control(s, svcFlag)
		if err != nil {
			log.Printf("Valid actions: %q\n", service.ControlAction)
			log.Fatal(err)
		}
		return
	}

	err = s.Run()
	errCheckFatal(err)

}
