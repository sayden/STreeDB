package fileformat

// func NewArrow[T Entry](dataWriter io.Writer, data Entries[T]) error {
// 	// create a schema for the data
// 	schema := aschema.FieldList{
// 		aschema.NewInt32Node("n", aparquet.Repetitions.Required, 0),
// 	}
// 	pqarrow.FromParquet(&schema)
//
// 	group, err := aschema.NewGroupNode("group_node", aparquet.Repetitions.Repeated, schema, 0)
// 	if err != nil {
// 		return err
// 	}
//
// 	// write data to file, create a new Arrow file
// 	parquetWriter := afile.NewParquetWriter(dataWriter, group)
//
// 	rg := parquetWriter.AppendRowGroup()
// 	aWriterProps := afile.NewWriterProperties()
// 	pqarrow.NewFileWriter(&schema, dataWriter, nil, aWriterProps)
//
// 	var col afile.ColumnChunkWriter
// 	// for (col != nil) && (cols > 0){
// 	for cols := rg.NumColumns(); (col != nil) && (cols > 0); cols-- {
// 		cw := col.(*afile.Int32ColumnChunkWriter)
// 		cw.WriteBatch(pqarrow.NewInt32Builder().AppendValues([]int32{1, 2, 3, 4, 5}).NewArray())
// 		col, err = rg.NextColumn()
// 	}
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }
