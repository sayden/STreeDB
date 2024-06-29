package fs

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/thehivecorporation/log"
	"github.com/zeebo/assert"
)

func TestArrow(t *testing.T) {
	t.Skip()
	pool := memory.NewGoAllocator()

	dtype := arrow.StructOf([]arrow.Field{
		{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
	}...)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.ListBuilder)
	f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	sb.Reserve(4)
	f1vb.Reserve(7)
	f2b.Reserve(3)

	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("joe"), nil)
	f2b.Append(1)

	sb.Append(true)
	f1b.AppendNull()
	f2b.Append(2)

	sb.AppendNull()

	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("mark"), nil)
	f2b.Append(4)

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	fmt.Printf("NullN() = %d\n", arr.NullN())
	fmt.Printf("Len()   = %d\n", arr.Len())

	list := arr.Field(0).(*array.List)
	offsets := list.Offsets()

	varr := list.ListValues().(*array.Uint8)
	ints := arr.Field(1).(*array.Int32)

	for i := 0; i < arr.Len(); i++ {
		if !arr.IsValid(i) {
			fmt.Printf("Struct[%d] = (null)\n", i)
			continue
		}
		fmt.Printf("Struct[%d] = [", i)
		pos := int(offsets[i])
		switch {
		case list.IsValid(pos):
			fmt.Printf("[")
			for j := offsets[i]; j < offsets[i+1]; j++ {
				if j != offsets[i] {
					fmt.Printf(", ")
				}
				fmt.Printf("%v", string(varr.Value(int(j))))
			}
			fmt.Printf("], ")
		default:
			fmt.Printf("(null), ")
		}
		fmt.Printf("%d]\n", ints.Value(i))
	}
}

// Define your struct
type Person struct {
	Name string
	Age  int32
}

func TestArrow2(t *testing.T) {
	t.Skip()
	log.SetLevel(log.LevelDebug)

	// Your Go array of structs
	people := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
	}

	// Create memory allocator
	pool := memory.NewGoAllocator()

	// Define Arrow schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Create Arrow record builder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Get builders for each field
	nameBuilder := builder.Field(0).(*array.StringBuilder)
	ageBuilder := builder.Field(1).(*array.Int32Builder)

	// Append data to builders
	for _, p := range people {
		nameBuilder.Append(p.Name)
		ageBuilder.Append(p.Age)
	}

	// Create the record
	record := builder.NewRecord()
	defer record.Release()

	// Open output file
	log.Info("Creating Parquet file")
	outputFile, err := os.Create("people.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	// Create Parquet writer
	writerProps := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	writer, err := pqarrow.NewFileWriter(schema, outputFile, writerProps, arrowProps)
	if err != nil {
		log.Fatal(err)
	}

	// Write the record
	if err := writer.Write(record); err != nil {
		log.Fatal(err)
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	log.Info("Successfully wrote Parquet file")
}

func TestArrowFromParquet(t *testing.T) {
	reader, err := file.OpenParquetFile("people.parquet", false)
	assert.NoError(t, err)

	// Create an Arrow memory allocator
	mem := memory.NewGoAllocator()

	fr, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	assert.NoError(t, err)
	arrowTable, err := fr.ReadTable(context.TODO())
	assert.NoError(t, err)

	// Process the Arrow table
	for _, field := range arrowTable.Schema().Fields() {
		fmt.Println("Field:", field.Name)
	}

	// Access the data from the Arrow table
	col := arrowTable.Column(0)

	data := col.Data()
	for _, chunk := range data.Chunks() {
		list := chunk.(*array.String)
		fmt.Printf("col: '%s' '%v' '%v'\n", col.Name(), data.Len(), list.String())
	}

	col = arrowTable.Column(1)

	data = col.Data()
	for _, chunk := range data.Chunks() {
		list := chunk.(*array.Int32)
		fmt.Printf("col: '%s' '%v' '%v'\n", col.Name(), data.Len(), list.Int32Values())
	}

	fmt.Println("Parquet file read successfully")
}
