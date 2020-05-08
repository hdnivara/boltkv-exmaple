package db

import (
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

// DB represents the key-value database and assoicated items.
type DB struct {
	// KV is the key-value database used to store ZIP->co-ordinate mappings.
	KV *bolt.DB

	// Bkt is the default Bolt KV DB bucket.
	Bkt []byte

	// Path is the location of KV database.
	Path string
}

// NewDB creates a new database.
func NewDB() *DB {
	return &DB{}
}

// Init opens a database at the given path and intialises it.
func (db *DB) Init(path string) error {
	db.Path = path

	kv, err := bolt.Open(db.Path, 0600, nil)
	if err != nil {
		return fmt.Errorf("opening KV database failed: %v", err)
	}
	db.KV = kv

	err = kv.Update(func(tx *bolt.Tx) error {
		db.Bkt = []byte("default")
		_, err = tx.CreateBucketIfNotExists(db.Bkt)
		if err != nil {
			return fmt.Errorf("creating default bucket failed: %v", err)
		}

		return nil
	})

	return err
}

// Cleanup closes and removes the database.
func (db *DB) Cleanup() {
	db.KV.Close()
	os.Remove(db.Path)
}

// Insert adds a new entry to the database.
func (db *DB) Insert(key, value []byte) error {
	err := db.KV.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(db.Bkt).Put(key, value)
		if err != nil {
			return fmt.Errorf("failed to insert: %v", err)
		}

		return nil
	})

	return err
}

// Remove deletes an entry for the given ZIP-code from the database.
func (db *DB) Remove(key []byte) error {
	err := db.KV.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(db.Bkt).Delete(key)
		if err != nil {
			return fmt.Errorf("failed to remove: %v", err)
		}

		return nil
	})

	return err
}

// Lookup reutrns an entry from database for given ZIP.
func (db *DB) Lookup(key []byte) ([]byte, error) {
	var value []byte

	err := db.KV.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(db.Bkt).Get(key)
		if v != nil {
			value = v
			return nil
		}

		return fmt.Errorf("cannot find entry")
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

// Dump pretty-prints all database entries.
func (db *DB) Dump(dumper func(k, v []byte)) {
	err := db.KV.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(db.Bkt)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			dumper(k, v)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}
