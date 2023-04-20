/*
import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)
*/

struct S3File {
    client: s3::Client,
    bucket: String,
    key: String,
    position: i64,
    length: i64
}

const UnknownSize: i64 = -1;

impl S3File {
    pub fn new(client: s3::Client, bucket: String, key: String, length: i64)
		-> Result<S3File>
	{
		let slf = Self {
			client,
			bucket,
			key,
			length,
		};

		if slf.length == UnknownSize {
			let out = client.HeadObject(ctx, &s3.HeadObjectInput {
				bucket: &bucket,
				key:    &key,
			})?;

			slf.length = out.ContentLength
		}
		Ok(slf)
	}

	pub fn read(&self, p: Vec<u8>) -> Result<i32> {
		self.read_at(p, self.position)
	}

	pub fn seek(offset: i64, whence: i32) -> Result<i64> {
		let position = 0 as i64;
		match whence {
			io.SeekStart => {
				position = offset;
			}
			io.SeekCurrent => {
				position = self.position + offset;
			}
			io.SeekEnd => {
				position = i64(self.length) + offset;
			}
		}

		if position < 0 {
			return Err("negative result position");
		}
		if position > (self.length as i64) {
			return Err("new position exceeds size of file");
		}
		self.position = position;
		Ok(position)
	}

	pub fn read_at(&self, p: Vec<u8>, off: i64) -> Result<i32> {
		let mut end = off + (p.len() as i64) - 1;
		if end >= self.length {
			end = self.length - 1;
		}

		let out = self.client.GetObject(&s3::GetObjectInput {
			bucket: &self.bucket,
			key: &self.key,
			range: format!("bytes={}-{}", off, end)
		})?;

		io.ReadAtLeast(out.Body, p, (out.ContentLength as i32))
	}
}
