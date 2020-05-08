package db

import (
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
)

func doCreateDB(name string) *DB {

	db := NewDB()
	err := db.Init(name)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func createDB() *DB {
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}

	return doCreateDB(tmpfile.Name())
}

func deleteDB(db *DB) {
	db.Cleanup()
}

func insert(db *DB, kv map[string]string) {
	for k, v := range kv {
		err := db.Insert([]byte(k), []byte(v))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func TestInit(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}

	db := doCreateDB(tmpfile.Name())
	defer deleteDB(db)

	// Ensure database file is created.
	_, err = os.Stat(tmpfile.Name())
	if err != nil && os.IsNotExist(err) {
		t.Fatal("database is not created")
	}
}

func TestCleanup(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}

	db := doCreateDB(tmpfile.Name())
	db.Cleanup()

	// Ensure database file is deleted.
	_, err = os.Stat(tmpfile.Name())
	if !os.IsNotExist(err) {
		t.Fatal("database is not deleted")
	}
}

func TestInsert(t *testing.T) {
	db := createDB()
	defer deleteDB(db)

	kv := make(map[string]string)
	kv["key0"] = "value0"
	kv["key1"] = "value1"

	// Insert test data in database.
	insert(db, kv)

	// Ensure lookups for inserted keys matches our copy.
	for k, expected := range kv {
		v, err := db.Lookup([]byte(k))
		if err != nil {
			log.Fatal(err)
		}

		actual := string(v)

		if expected != actual {
			log.Fatalf("key=%s: expected=%s actual=%s\n", k, expected, actual)
		}
	}
}

func TestRemove(t *testing.T) {
	db := createDB()
	defer deleteDB(db)

	kv := make(map[string]string)
	kv["key0"] = "value0"
	kv["key1"] = "value1"

	// Insert test data in database.
	insert(db, kv)

	// Remove one of the keys and then do a lookup. The lookup should
	// fail.
	err := db.Remove([]byte(kv["key0"]))
	if err != nil {
		log.Fatal(err)
	}

	v, err := db.Lookup([]byte(kv["key0"]))
	if err == nil {
		t.Fatalf("lookup succeeds after removing key; actual=%s expected=nil\n",
			string(v))
	}
}

func TestDump(t *testing.T) {
	db := createDB()
	defer deleteDB(db)

	actual := make(map[string]string)
	kv := make(map[string]string)
	kv["key0"] = "value0"
	kv["key1"] = "value1"

	// Insert test data in database.
	insert(db, kv)

	// Even though Dump is used for pretty-printing, we use it re-create
	// KV pairs using the data provided in Dump and then compare Dump's
	// data with what we inserted.
	db.Dump(func(k, v []byte) {
		key := string(k)
		value := string(v)
		actual[key] = value
	})

	if !reflect.DeepEqual(kv, actual) {
		t.Fatalf("expected={%v} actual={%v}\n", kv, actual)
	}
}
