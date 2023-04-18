/*
import (
	"github.com/PacktPublishing/In-Memory-Analytics-with-Apache-Arrow-/utils"
	"github.com/apache/arrow/go/v8/arrow/arrio"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
*/

use arrow::{
    flight::{Criteria, FlightInfo, Ticket, FlightDescriptor, FlightEndpoint},
    ipc
};
use parquet;
use aws_config::;
use aws_sdk_s3::Client as S3Client;

struct Server {
	// flight.BaseFlightServer,
	s3_client: S3Client,
	bucket: String
}

impl Server {
    pub fn new() -> Self {
        Self {
            s3_client: S3Client::new(s3::Options { region: "us-east-2" }),
            bucket: "ursa-labs-taxi-data".to_owned(),
        }
    }

    pub async fn list_flights(&self, c: Criteria, fs: flight.FlightService_ListFlightsServer) -> error {
        let mut prefix = String;
        if c.expression.len() > 0 {
            prefix = c.expression.to_string()
        }
        let list = self.s3_client.list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send().await
            .unwrap();
    
        for content in list.contents().unwrap() {
            let key = content.key().unwrap();
            if !key.ends_with(".parquet") {
                continue
            }
            let info = self.get_flight_info(fs.Context(), key, content.size()).unwrap();
            fs.Send(info).unwrap();
        }
    
        Ok(())
    }

    fn get_flight_info(ctx: context.Context, key: &str, size: i64)
        -> Result<FlightInfo>
    {
        let s3file = utils.NewS3File(ctx, self.s3_client,
            self.bucket, key, size).unwrap();
    
        let pr = file.NewParquetReader(s3file).unwrap();
        // defer pr.Close()
    
        let sc = pqarrow.FromParquet(pr.MetaData().Schema, nil, nil).unwrap();
    
        Ok(FlightInfo {
            schema: flight.SerializeSchema(sc, memory.DefaultAllocator),
            flight_descriptor: &FlightDescriptor {
                r#type: flight.DescriptorPATH,
                path: vec![key],
            },
            endpoint: vec![FlightEndpoint {{
                ticket: Some(Ticket { ticket: []byte(key) }),
                location: Vec::new()
            }}],
            total_records: pr.NumRows(),
            total_bytes: -1,
        })
    }

    pub fn do_get(&self, tkt: Ticket, fs: flight.FlightService_DoGetServer) error {
        let path = string(tkt.ticket)
        let sf = utils.NewS3File(fs.Context(), self.s3_client,
            self.bucket, path, utils.UnknownSize).unwrap();
        let pr = file.NewParquetReader(sf).unwrap();
        defer pr.Close()
        let arrowRdr = pqarrow.NewFileReader(pr,
            pqarrow.ArrowReadProperties { parallel: true, batch_size: 100000 },
            memory.DefaultAllocator).unwrap();
        let rr = arrowRdr.GetRecordReader(fs.Context(), nil, nil).unwrap();
        // defer rr.Release()
        let wr = flight.NewRecordWriter(fs, ipc.WithSchema(rr.Schema()));
        // defer wr.Close()
        let n, err = arrio.Copy(wr, rr);
        println!("wrote", n, "record batches");
    
        err
    }
}


fn main() {
	let srv = flight.NewServerWithMiddleware(nil);
	srv.Init("0.0.0.0:0");
	srv.RegisterFlightService(NewServer());
	// the Serve function doesn’t return until the server
	// shuts down. For now we’ll start it running in a goroutine
	// and shut the server down when our main ends.
	go srv.Serve()
	defer srv.Shutdown()

	let client = flight.NewClientWithMiddleware(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials())).unwrap();
	defer client.Close()

	let info_stream = client.ListFlights(context.TODO(),
		&flight.Criteria{Expression: []byte("2009")}).unwrap();

	loop {
		let info, err = info_stream.Recv();
		if err != nil {
			if err == io.EOF { // we hit the end of the stream
				break
			}
			panic(err) // we got an error!
		}
		println!(info.GetFlightDescriptor().GetPath());
	}

}
