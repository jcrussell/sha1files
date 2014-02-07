package main

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Information about the file that will be stored in the sqlite database.
type record struct {
	extless string
	ext     string
	sha1    string
	path    string
}

// Compute the SHA1 hash of a file specified by its path. It will return the SHA1 or
// an empty string and the error that occured.
func calcSha1(path string) (string, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	hasher := sha1.New()
	hasher.Write(bytes)
	hash := hex.EncodeToString(hasher.Sum(nil))

	return hash, nil
}

// Visit a file in the directory tree, return a new record for the file or
// the error that occured. If the file starts with a ".", an error will be
// returned indicating that the file/directory should be skipped.
func doVisit(path string, info os.FileInfo, err error) (*record, error) {
	if strings.HasPrefix(info.Name(), ".") {
		// Skip hidden files and directories
		return nil, filepath.SkipDir
	}

	if info.IsDir() {
		log.Printf("Descending into dir: %s\n", info.Name())
		return nil, nil
	} else {
		ext := filepath.Ext(info.Name())
		extless := strings.Replace(info.Name(), ext, "", -1)

		sha1, err := calcSha1(path)
		if err != nil {
			return nil, err
		}

		return &record{
			extless: extless,
			ext:     ext,
			sha1:    sha1,
			path:    path,
		}, nil
	}
}

// Insert a batch of records into files table in SQLite. Returns any errors
// that occurred or nil if there were none.
func commitRecords(db *sql.DB, records []*record) error {
	log.Printf("Commmitting batch of %d records\n", len(records))

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := db.Prepare("INSERT INTO files (extless, ext, sha1, path) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		stmt.Exec(record.extless, record.ext, record.sha1, record.path)
	}

	tx.Commit()
	return nil
}

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Printf("USAGE: sha1files DIR [DIR]...\n")
		return
	}

	db, err := sql.Open("sqlite3", "./files.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sql := "CREATE TABLE IF NOT EXISTS files (extless TEXT, ext CHAR(3), sha1 CHAR(40), path TEXT)"
	_, err = db.Exec(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
		return
	}

	records := []*record{}

	visit := func(path string, info os.FileInfo, err error) error {
		result, suberr := doVisit(path, info, err)
		if suberr != nil {
			return suberr
		} else if result != nil {
			records = append(records, result)
		}

		// Commit in batches of 100000
		if len(records) == 100000 {
			err := commitRecords(db, records)
			if err != nil {
				log.Fatal(err)
			}
			records = []*record{}
		}

		return nil
	}

	for _, dir := range flag.Args() {
		abs, err := filepath.Abs(dir)
		if err != nil {
			log.Printf("Error processing dir: %s\n", dir)
		}

		filepath.Walk(abs, visit)
	}

	// Commit any remaining records
	if len(records) > 0 {
		err := commitRecords(db, records)
		if err != nil {
			log.Fatal(err)
		}
	}
}
