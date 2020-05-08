package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"boltkv-exmaple/db"
)

// Location represents the geographical co-ordinates: latitude,
// longitude.
type Location struct {
	// Latitude of a ZIP-code.
	Latitude string

	// Longitude to a ZIP-code.
	Longitude string
}

type GeoZip struct {
	// ZipCode is the ZIP-code to be used as key in KV store.
	ZipCode uint

	// GeoLoc is the geographical co-ordinates of ZipCode and this is
	// value in KV store.
	GeoLoc Location
}

func printUnderline(hdr string) {
	fmt.Println(hdr)
	for i := 0; i < len(hdr); i++ {
		fmt.Printf("=")
	}
	fmt.Println()
}

func bytesToGeoZip(b []byte) (*GeoZip, error) {
	g := GeoZip{}

	decBuf := bytes.NewBuffer(b)
	err := gob.NewDecoder(decBuf).Decode(&g)
	if err != nil {
		return nil, err
	}

	return &g, nil
}

func bytesFromGeoZip(g *GeoZip) ([]byte, error) {
	encBuf := new(bytes.Buffer)
	err := gob.NewEncoder(encBuf).Encode(g)
	if err != nil {
		return nil, err
	}

	return encBuf.Bytes(), nil
}

func newGeoZip(zip uint) *GeoZip {
	// Mock co-ordinates for now.
	// Replace with real values later.
	lat := uint64(zip * 1024)
	lon := uint64(zip * 2048)

	loc := Location{
		Latitude:  strconv.FormatUint(lat, 10),
		Longitude: strconv.FormatUint(lon, 10),
	}

	return &GeoZip{ZipCode: zip, GeoLoc: loc}
}

func insertZip(db *db.DB, zip uint) {
	g := newGeoZip(zip)
	k := []byte(string(zip))

	v, err := bytesFromGeoZip(g)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Insert(k, v)
	if err != nil {
		log.Fatal(err)
	}
}

func removeZip(db *db.DB, zip uint) {
	k := []byte(string(zip))

	err := db.Remove(k)
	if err != nil {
		log.Fatal(err)
	}
}

func lookupZip(db *db.DB, zip uint) (*GeoZip, error) {
	k := []byte(string(zip))

	b, err := db.Lookup(k)
	if err != nil {
		return nil, err
	}

	g, err := bytesToGeoZip(b)
	if err != nil {
		return nil, err
	}

	return g, nil
}

func dumpZip(db *db.DB) {
	db.Dump(printBytesAsGeoZip)
}

func printBytesAsGeoZip(k, v []byte) {
	g, err := bytesToGeoZip(v)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(g)
}

func main() {
	db := db.NewDB()

	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Init(tmpfile.Name())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Cleanup()

	zips := []uint{27606, 95134, 95054, 86001}

	printUnderline("Add to DB")
	for _, zip := range zips {
		fmt.Printf("Adding zip=%d.. ", zip)
		insertZip(db, zip)
		fmt.Println("done")
	}
	fmt.Println()

	printUnderline("Dump DB")
	dumpZip(db)
	fmt.Println()

	printUnderline("Lookup DB")
	for _, zip := range zips {
		fmt.Printf("Looking up zip=%d.. ", zip)
		geo, err := lookupZip(db, zip)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		} else {
			fmt.Printf("%v\n", geo)
		}
	}
	fmt.Println()

	printUnderline("Remove from DB")
	fmt.Printf("Removing zip=%d.. ", zips[0])
	removeZip(db, zips[0])
	fmt.Println("done")
	fmt.Println()

	printUnderline("Dump DB")
	dumpZip(db)
	fmt.Println()
}
