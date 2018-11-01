package main

import (
	"database/sql"
	"io/ioutil"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqldumper "github.com/hunzai/go-mysqldumper"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stdout)

	log.SetLevel(log.DebugLevel)

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = time.RFC3339Nano
	log.SetFormatter(customFormatter)
}

func main() {
	configFile := "../../config.json"

	// read the config file
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	// parse config file
	config, err := mysqldumper.ParseConfig(configData)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the production DB
	dsn := config.Source.DSN
	log.Info("Connecting on ", dsn)
	db, err := NewDB("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	// create new dumper
	dumper := mysqldumper.New(config, db, log.StandardLogger())

	// create dump file
	outputFile := config.Target.FilePath
	if outputFile == "" {
		log.Fatal("Output file name doesn't exists")
	}
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// create new dump writer
	w := mysqldumper.NewFileWriter(f)
	defer w.Flush() // flush at the end

	// start dumping the data
	err = dumper.Dump(w)
	if err != nil {
		log.Fatal(err)
	}

}

func NewDB(driver, connectionString string) (*sql.DB, error) {
	db, err := sql.Open(driver, connectionString)
	if err != nil {
		return nil, err
	}

	return db, db.Ping()
}
