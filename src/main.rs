#[macro_use]
extern crate lazy_static;

use regex::Regex;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::EnvironmentProvider;
use rusoto_s3::{
    GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
// use std::io::Read;
use std::str::FromStr;
use structopt::StructOpt;
use tokio::prelude::{future, Future};
use tokio::runtime::{self, Runtime};

type Error = Box<dyn std::error::Error>;

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

// impl S3Path {
//     pub fn is_dir(&self) -> bool {
//         self.key.ends_with("/")
//     }
// }

impl FromStr for S3Path {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"^s3://(.+?)(?:/(.*))?$").unwrap();
        }

        let caps = RE.captures(s).unwrap();
        let bucket = caps.get(1).unwrap().as_str().to_owned();

        let key = match caps.get(2) {
            Some(key) => key.as_str().to_owned(),
            None => "".to_owned(),
        };

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

fn cp_action(
    rt: &mut Runtime,
    s3: &rusoto_s3::S3Client,
    src_path: &S3Path,
    matching_objs: impl Iterator<Item = rusoto_s3::Object>,
) -> Result<(), Error> {
    let get_obj_output_futs = matching_objs.map(move |matching_obj| {
        let get_obj_req = GetObjectRequest {
            bucket: src_path.bucket.clone(),
            key: matching_obj.key.clone().unwrap(),
            ..Default::default()
        };

        let rel_path = get_obj_req
            .key
            .trim_start_matches(&src_path.key)
            .trim_start_matches('/')
            .to_owned();

        s3.get_object(get_obj_req)
            .map(|fut| (rel_path, fut))
            .map_err(|e| -> Error { e.into() })
    });

    let put_obj_output_futs = get_obj_output_futs.map(|get_obj_output_fut| {
        // TODO and_then
        get_obj_output_fut.map(|(key, get_obj_output)| {
            println!(
                "{}, content-length: {}",
                key,
                get_obj_output.content_length.unwrap()
            );

            get_obj_output

            // // dst
            // let put_obj_req = PutObjectRequest {
            //     bucket: dst_path.bucket.clone(),
            //     key: dst_path.key.clone(),
            //     body: get_obj_output.body,
            //     content_disposition: get_obj_output
            //         .content_disposition,
            //     content_language: get_obj_output
            //         .content_language,
            //     content_length: get_obj_output.content_length,
            //     // content_md5: get_obj_output.content_md5,
            //     content_type: get_obj_output.content_type,
            //     metadata: get_obj_output.metadata,
            //     ..Default::default()
            // };

            // dst_s3
            //     .put_object(put_obj_req)
            //     .map_err(|e| -> Error { e.into() })
        })
    });

    for put_obj_output_fut in put_obj_output_futs {
        rt.spawn(put_obj_output_fut.map(|_| ()).map_err(|e| {
            eprintln!("Future error: {}", e);
            ()
        }));
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::from_args();
    let provider = EnvironmentProvider::default();
    let dst_provider = EnvironmentProvider::with_prefix("DST_AWS");

    let s3 =
        S3Client::new_with(HttpClient::new()?, provider, Region::ApSoutheast1);

    let dst_s3 = S3Client::new_with(
        HttpClient::new()?,
        dst_provider,
        Region::ApSoutheast1,
    );

    match args.subcommand {
        Subcommand::Cp { src, dst } => {
            let src_path = S3Path::from_str(&src)?;
            let dst_path = S3Path::from_str(&dst)?;

            let mut rt = runtime::Builder::new().core_threads(4).build()?;

            // Get initial matching prefixes objects
            let mut is_truncated = true;
            let mut next_continuation_token = None;

            while is_truncated {
                let list_objs_req = ListObjectsV2Request {
                    bucket: src_path.bucket.clone(),
                    prefix: Some(src_path.key.clone()),
                    continuation_token: next_continuation_token,
                    ..Default::default()
                };

                let list_obj_output =
                    s3.list_objects_v2(list_objs_req).sync()?;
                let matching_objs =
                    list_obj_output.contents.unwrap().into_iter();

                // Get all object contents
                cp_action(&mut rt, &s3, &src_path, matching_objs)?;

                is_truncated = list_obj_output.is_truncated.unwrap_or(false);
                next_continuation_token =
                    list_obj_output.next_continuation_token.clone();
            }

            rt.shutdown_on_idle().wait().unwrap();
        }
    }

    Ok(())
}
