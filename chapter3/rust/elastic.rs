use std::{
	collections::HashMap,
	fs::File
};
use arrow::{
	array,
	datatypes::{DataType, Field, Schema},
	json::writer
};
use parquet::{
	arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
	file
};
use elasticsearch::{BulkOperation, Elasticsearch};

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
    
    // leave these empty since we're not filtering out any
    // columns or row groups. But if you wanted to do so,
    // this is how you'd optimize the read
    let cols: Vec<i32>;
	let rowgroups: Vec<i32>;
    let rr = reader.GetRecordReader(cols, rowgroups).unwrap();

	let client = Elasticsearch::default();
    let response = client.indices().create("indexname").body(json!({
		"mappings": {
			"properties": create_mapping(reader.schema())
		}
	})).send().await?;
    if !response.status_code().is_success() {
        // handle failure response and return/exit
        panic!("non-ok status: {}", response.status_code());
    }
    // Index created!

    let indexer = BulkIndexOperation::new()
		.index("indexname")
		// OnError: |_: context.Context, err: error| println!(err),
		.unwrap();

    let pr, pw = io.Pipe();  // to pass the data
    go func() {
        while let Some(batch) = reader.next() {
            if err = array.RecordToJSON(batch, pw); err != nil {
                cancel()
                pw.CloseWithError(err)
                return
            }
        }
        pw.Close()
    }()

    let scanner = bufio.NewScanner(pr);
    while scanner.scan() {
        indexer.Add(ctx, esutil.BulkIndexerItem {
            action: "index",
            body: strings.NewReader(scanner.text()),
            on_failure: |_: context.Context,
                item: esutil.BulkIndexerItem,
                resp: esutil.BulkIndexerResponseItem,
                err: error| print!("Failure! {}, {}\n{}\n", err, item, resp),
        }).unwrap();
    }

    indexer.close(ctx).unwrap();
}
