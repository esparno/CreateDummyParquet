package main

import (
	"fmt"
	"log"

	randomdata "github.com/Pallinder/go-randomdata"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

func main() {
	const filename = "dummy.parquet"
	const numrecords = 100
	CreateDummyParquet(numrecords, filename)
	ReadDummyParquet(filename)
}

type Order struct {
	OrderID    int32   `parquet:"name='OrderID', type=INT32"`
	FirstName  string  `parquet:"name=FirstName, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LastName   string  `parquet:"name=LastName, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Email      string  `parquet:"name=Email, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Quantity   int32   `parquet:"name='Quantity', type=INT32"`
	OrderTotal float64 `parquet:"name='OrderTotal', type=DOUBLE"`
}

func CreateDummyParquet(numRecords int, filename string) {
	var err error
	fw, err := ParquetFile.NewLocalFileWriter(filename)
	defer fw.Close()
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, new(Order), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < numRecords; i++ {
		o := Order{
			OrderID:    int32(randomdata.Number(100000, 999999)),
			FirstName:  randomdata.FirstName(randomdata.RandomGender),
			LastName:   randomdata.LastName(),
			Email:      randomdata.Email(),
			Quantity:   int32(randomdata.Number(1, 10)),
			OrderTotal: randomdata.Decimal(0, 300, 2),
		}
		if err = pw.Write(o); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")

}

func ReadDummyParquet(filename string) {
	fr, err := ParquetFile.NewLocalFileReader(filename)
	defer fr.Close()
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetReader(fr, new(Order), 4)
	defer pr.ReadStop()
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	rows := int(pr.GetNumRows())
	for i := 0; i < rows; i++ {
		orders := make([]Order, 1, rows)
		if err = pr.Read(&orders); err != nil {
			log.Println("Read error", err)
		}
		fmt.Println(orders)
	}
}
