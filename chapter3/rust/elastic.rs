use arrow::datatypes::{DataType, Schema};
use parquet;

struct Property {
    ty: String,
	// #[omitempty]
    format: String,
	// #[omitempty]
    fields: interface{}
}

var keywordField = &struct {
	keyword interface{}
}{struct {
	ty: string,
	ignore_above: i32
}{"keyword", 256}}

var primitiveMapping = HashMap<arrow::Type, String> {
	arrow.BOOL:       "boolean",
	arrow.INT8:       "byte",
	arrow.UINT8:      "short", // no unsigned byte type
	arrow.INT16:      "short",
	arrow.UINT16:     "integer", // no unsigned short
	arrow.INT32:      "integer",
	arrow.UINT32:     "long", // no unsigned integer
	arrow.INT64:      "long",
	arrow.UINT64:     "unsigned_long",
	arrow.FLOAT16:    "half_float",
	arrow.FLOAT32:    "float",
	arrow.FLOAT64:    "double",
	arrow.DECIMAL:    "scaled_float",
	arrow.DECIMAL256: "scaled_float",
	arrow.BINARY:     "binary",
	arrow.STRING:     "text",
}

fn create_mapping(sc: Schema) -> HashMap<String, Property> {
	let mut mappings = HashMap::new();
	for (_, f) in range sc.fields() {
		var (
			p  property
			ok bool
		)
		if p.Type, ok = primitiveMapping[f.Type.ID()]; !ok {
			match f.Type.ID() {
				arrow.DATE32 | arrow.DATE64 =>
					p.Type = "date"
					p.Format = "yyyy-MM-dd",
				arrow.TIME32 | arrow.TIME64  =>
					p.Type = "date"
					p.Format = "time||time_no_millis",
				arrow.TIMESTAMP =>
					p.Type = "date"
					p.Format = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
				arrow.STRING =>
					p.Type = "text" // or keyword
					p.Fields = keywordField
			}
		}
		mappings[f.Name] = p
	}
	mappings
}

fn main() {
	// the second argument is a bool value for memory mapping
	// the file if desired.
  	let parq = file.OpenParquetFile("../../sample_data/sliced.parquet", false).unwrap();
	// defer parq.Close()

	let props = pqarrow.ArrowReadProperties { BatchSize: 50000 };
	let rdr = pqarrow.NewFileReader(parq, props,
		memory.DefaultAllocator).unwrap();

	let ctx, cancel = context.WithCancel(context.Background());
	// defer cancel()
	
	// leave these empty since we're not filtering out any
	// columns or row groups. But if you wanted to do so,
	// this is how you'd optimize the read
	var cols, rowgroups []int
	let rr = rdr.GetRecordReader(ctx, cols, rowgroups).unwrap();

	let es = elasticsearch.NewDefaultClient().unwrap();

	let mut mapping = struct {
		mappings struct {
			properties HashMap<String, Property>
		}
	}
	mapping.mappings.properties = create_mapping(rr.schema());
	let response = es.Indices.Create("indexname",
		es.Indices.Create.WithBody(
			esutil.NewJSONReader(mapping))).unwrap();
	if response.StatusCode != http.StatusOK {
		// handle failure response and return/exit
		panic(fmt.Errorf("non-ok status: %s", response.Status()))
	}
	// Index created!

	let indexer = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es, Index: "indexname",
		OnError: func(_ context.Context, err error) {
			fmt.Println(err)
		},
	}).unwrap();

	let pr, pw = io.Pipe() // to pass the data
	go func() {
		for rr.Next() {
			if err = array.RecordToJSON(rr.record(), pw); err != nil {
				cancel()
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()

	scanner = bufio.NewScanner(pr)
	for scanner.Scan() {
		indexer.Add(ctx, esutil.BulkIndexerItem{
			Action: "index",
			Body:   strings.NewReader(scanner.Text()),
			OnFailure: func(_ context.Context,
				item esutil.BulkIndexerItem,
				resp esutil.BulkIndexerResponseItem,
				err error) {
				fmt.Printf("Failure! %s, %+v\n%+v\n", err, item, resp)
			},
		}).unwrap();
	}

	indexer.close(ctx).unwrap();
}
