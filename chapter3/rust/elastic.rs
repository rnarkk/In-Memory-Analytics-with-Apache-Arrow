use std::{
	collections::HashMap,
	fs::File
};
use arrow::{
	array,
	datatypes::{DataType, Field, Schema}
};
use parquet::{
	arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
	file
};
use elasticsearch::Elasticsearch;

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

static primitiveMapping: HashMap<DataType, String> = HashMap::from(vec![
    (DataType::Bool, "boolean"),
    (DataType::Int8, "byte"),
    (DataType::Uint8, "short"),  // no unsigned byte type
    (DataType::Int16, "short"),
    (DataType::Uint16, "integer"), // no unsigned short
    (DataType::Int32, "integer"),
    (DataType::Uint32, "long"), // no unsigned integer
    (DataType::Int64, "long"),
    (DataType::Uint64, "unsigned_long"),
    (DataType::Float16, "half_float"),
    (DataType::Float32, "float"),
    (DataType::Float64, "double"),
    (DataType::Decimal, "scaled_float"),
    (DataType::Decimal256, "scaled_float"),
    (DataType::Binary, "binary"),
    (DataType::String, "text"),
]);

fn create_mapping(schema: Schema) -> HashMap<String, Property> {
    let mut mappings = HashMap::new();
    for field in schema.fields() {
        let p: property;
        if primitive_mapping.get(field.data_type()).is_none() {
            match field.data_type() {
                DataType::Date32 | DataType::Date64 => {
                    p.Type = "date";
                    p.Format = "yyyy-MM-dd";
				}
                DataType::Time32 | DataType::Time64  => {
                    p.Type = "date";
                    p.Format = "time||time_no_millis";
				}
                DataType::Timestamp => {
                    p.Type = "date";
                    p.Format = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
				}
                DataType::String => {
                    p.Type = "text";  // or keyword
                    p.Fields = keywordField;
				}
            }
        }
        mappings.insert(field.name(), p);
    }
    mappings
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // the second argument (false) is a bool value for memory mapping
    // the file if desired.
	let file = File::open("../../sample_data/sliced.parquet").unwrap();
	let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
		.with_batch_size(50000).build().unwrap();

    let ctx, cancel = context.WithCancel(context.Background());
    // defer cancel()
    
    // leave these empty since we're not filtering out any
    // columns or row groups. But if you wanted to do so,
    // this is how you'd optimize the read
    let cols: Vec<i32>;
	let rowgroups: Vec<i32>;
    let rr = reader.GetRecordReader(ctx, cols, rowgroups).unwrap();

    let es = elasticsearch.NewDefaultClient().unwrap();
	let client = Elasticsearch::default();

    let mut mapping = struct {
        mappings struct {
            properties HashMap<String, Property>
        }
    }
    mapping.mappings.properties = create_mapping(rr.schema());
    let response = client.indices().create("indexname").body(json!({
		"mappings": {
			"properties": {
				"field1": { "type" : "text" }
			}
		}
	})).send().await?;
    if !response.status_code().is_success() {
        // handle failure response and return/exit
        panic!("non-ok status: {}", response.status_code());
    }
    // Index created!

    let indexer = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
        Client: client, Index: "indexname",
        OnError: func(_ context.Context, err error) {
            fmt.Println(err)
        },
    }).unwrap();

    let pr, pw = io.Pipe();  // to pass the data
    go func() {
        for rr.next() {
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
