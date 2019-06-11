#[macro_use]
extern crate lazy_static;

use regex::Regex;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::EnvironmentProvider;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use std::io::Read;
use std::str::FromStr;
use structopt::StructOpt;

type Error = Box<std::error::Error>;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Rustree",
    about = "Rust-based S3 CLI catered for object transfers"
)]
struct Args {
    /// Subcommand option
    #[structopt(subcommand)]
    subcommand: Subcommand,
}

struct S3Path {
    pub bucket: String,
    pub key: String,
}

impl S3Path {
    pub fn is_dir(&self) -> bool {
        self.key.ends_with("/")
    }
}

impl FromStr for S3Path {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"^s3://(.+?)(?:/(.+))?$").unwrap();
        }

        let caps = RE.captures(s).unwrap();

        let bucket = caps.get(1).unwrap().as_str().to_owned();
        let key = caps.get(2).unwrap().as_str().to_owned();

        Ok(S3Path { bucket, key })
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "Rustree subcommand", about = "Rustree subcommand options")]
enum Subcommand {
    #[structopt(
        name = "cp",
        about = "Copy object from bucket to another bucket"
    )]
    Cp {
        /// Source object path to copy from
        #[structopt()]
        src: String,

        /// Source object path to copy from
        #[structopt()]
        dst: String,
    },
}

fn main() -> Result<(), Error> {
    let args = Args::from_args();
    let provider = EnvironmentProvider::default();

    let s3 =
        S3Client::new_with(HttpClient::new()?, provider, Region::ApSoutheast1);

    match args.subcommand {
        Subcommand::Cp { src, dst } => {
            let src_path = S3Path::from_str(&src)?;
            let dst_path = S3Path::from_str(&dst)?;

            let obj_req = GetObjectRequest {
                bucket: src_path.bucket.clone(),
                key: src_path.key.clone(),
                ..Default::default()
            };

            let obj_output = s3.get_object(obj_req).sync()?;

            let mut stream = obj_output.body.unwrap().into_blocking_read();
            let mut body = Vec::new();
            stream.read_to_end(&mut body)?;

            println!("{}", body.len());
        }
    }

    Ok(())
}
