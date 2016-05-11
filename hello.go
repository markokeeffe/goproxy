package main

import "fmt"
import "net/http"
import "encoding/json"

func main() {

	const TaskTypeDbMysqlQuery = 1
	const TaskTypeDbMysqlExec = 2

	type MysqlTaskConfig struct{
		Dsn		string		`json:"dsn"`
	}
	type Task struct{
		RawConfig	json.RawMessage	`json:"config"`
		Type		uint64		`json:"type"`
		Payload		string		`json:"payload"`

		Config		MysqlTaskConfig
	}


	resp, err := http.Get("http://taskserver:8888/")
	if err != nil {
		panic(err)
	}

	var task Task
	err = json.NewDecoder(resp.Body).Decode(&task)
	if err != nil {
		panic(err)
	}

	switch task.Type {
	case TaskTypeDbMysqlQuery, TaskTypeDbMysqlExec:
		var config MysqlTaskConfig
		err = json.Unmarshal(task.RawConfig, &config)
		if err != nil {
			panic(err)
		}
		task.Config = config;
	}


	fmt.Println(task.Config)
}