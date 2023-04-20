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
    context: context.Context,
    client: s3::Client,
    bucket: String,
    key: String,
    position: i64,
    length: i64
}

const UnknownSize: i64 = -1;

impl S3File {
    pub fn new(ctx: context.Context, client: *s3.Client, bucket: String,
			   key: String, length: i64) -> Result<S3File> {
		let slf = Self {
			context: ctx,
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
		let new_pos, offs = i64(0), offset;
		match whence {
			io.SeekStart => {
				new_pos = offset
			}
			io.SeekCurrent => {
				new_pos = self.position + offset
			}
			io.SeekEnd => {
				new_pos = i64(self.length) + offset
			}
		}

		if new_pos < 0 {
			return Err("negative result position")
		}
		if new_pos > i64(self.length) {
			return Err("new position exceeds size of file")
		}
		self.position = new_pos;
		Ok(new_pos)
	}

	pub fn read_at(&self, p: Vec<u8>, off: i64) -> Result<i32> {
		let end = off + (p.len() as i64) - 1;
		if end >= self.length {
			end = self.length - 1
		}

		let out = self.client.GetObject(self.Context, &s3::GetObjectInput {
			bucket: &self.bucket,
			key: &self.key,
			range: format!("bytes={}-{}", off, end)
		})?;

		io.ReadAtLeast(out.Body, p, i32(out.ContentLength))
	}
}
