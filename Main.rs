use clap::Parser;
use rayon::prelude::*;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

#[derive(Debug)]
enum Provider {
    AwsS3,
    AzureBlob,
    GcpStorage,
    DigitalOceanSpaces,
    LinodeObjStorage,
    Unknown,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input file containing bucket names (one per line). If not specified, bucket names are read from stdin.
    #[arg(short = 'i', long)]
    input: Option<String>,
}

struct BucketTarget {
    provider: Provider,
    bucket: String,
}

fn print_banner() {
    let banner_line1 = " ██▄ █ █ ▄▀▀ █▄▀ ██▀ ▀█▀ ▄▀▀ █▄█ ██▀ ▄▀▀ █▄▀ ██▀ █▀▄";
    let banner_line2 = " █▄█ ▀▄█ ▀▄▄ █ █ █▄▄  █  ▀▄▄ █ █ █▄▄ ▀▄▄ █ █ █▄▄ █▀▄";
    println!("\x1b[32m{}\x1b[0m", banner_line1);
    println!("\x1b[32m{}\x1b[0m", banner_line2);
}

// Identify provider and extract bucket/container name
fn extract_target(line: &str) -> Option<BucketTarget> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    // AWS S3
    if trimmed.ends_with(".s3.amazonaws.com") {
        let bucket = trimmed.trim_end_matches(".s3.amazonaws.com").to_string();
        return Some(BucketTarget { provider: Provider::AwsS3, bucket });
    }
    if trimmed.starts_with("s3.amazonaws.com/") {
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() >= 2 {
            return Some(BucketTarget { provider: Provider::AwsS3, bucket: parts[1].to_string() });
        }
    }

    // DigitalOcean Spaces (region.digitaloceanspaces.com)
    if trimmed.ends_with(".digitaloceanspaces.com") {
        let bucket = trimmed.split('.').next().unwrap_or(trimmed).to_string();
        return Some(BucketTarget { provider: Provider::DigitalOceanSpaces, bucket });
    }

    // Linode Object Storage (region.linodeobjects.com)
    if trimmed.ends_with(".linodeobjects.com") {
        let bucket = trimmed.split('.').next().unwrap_or(trimmed).to_string();
        return Some(BucketTarget { provider: Provider::LinodeObjStorage, bucket });
    }

    // Azure Blob ([container].blob.core.windows.net)
    if trimmed.ends_with(".blob.core.windows.net") {
        let container = trimmed.split('.').next().unwrap_or(trimmed).to_string();
        return Some(BucketTarget { provider: Provider::AzureBlob, bucket: container });
    }

    // GCP ([bucket].storage.googleapis.com or storage.googleapis.com/[bucket])
    if trimmed.ends_with(".storage.googleapis.com") {
        let bucket = trimmed.trim_end_matches(".storage.googleapis.com").to_string();
        return Some(BucketTarget { provider: Provider::GcpStorage, bucket });
    }
    if trimmed.starts_with("storage.googleapis.com/") {
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() >= 2 {
            return Some(BucketTarget { provider: Provider::GcpStorage, bucket: parts[1].to_string() });
        }
    }

    // Fallback: try as AWS, else unknown
    Some(BucketTarget { provider: Provider::Unknown, bucket: trimmed.to_string() })
}

fn construct_read_url(t: &BucketTarget) -> String {
    match t.provider {
        Provider::AwsS3 | Provider::DigitalOceanSpaces | Provider::LinodeObjStorage => format!("http://{}.{}", t.bucket, get_provider_domain(&t.provider)),
        Provider::AzureBlob => format!("https://{}.blob.core.windows.net/?restype=container&comp=list", t.bucket),
        Provider::GcpStorage => format!("https://storage.googleapis.com/{}/", t.bucket),
        Provider::Unknown => format!("http://{}", t.bucket),
    }
}

fn construct_write_url(t: &BucketTarget) -> String {
    let test_object = "codecompanion-test-object.txt";
    match t.provider {
        Provider::AwsS3 | Provider::DigitalOceanSpaces | Provider::LinodeObjStorage => format!("http://{}.{}{}", t.bucket, get_provider_domain(&t.provider), format!("/{}", test_object)),
        Provider::AzureBlob => format!("https://{}.blob.core.windows.net/{}", t.bucket, test_object),
        Provider::GcpStorage => format!("https://storage.googleapis.com/{}/{}", t.bucket, test_object),
        Provider::Unknown => format!("http://{}/{}", t.bucket, test_object),
    }
}

fn get_provider_domain(provider: &Provider) -> &'static str {
    match provider {
        Provider::AwsS3 => "s3.amazonaws.com",
        Provider::DigitalOceanSpaces => "nyc3.digitaloceanspaces.com", // default region; modify as needed
        Provider::LinodeObjStorage => "us-east-1.linodeobjects.com",
        _ => "",
    }
}

fn provider_str(provider: &Provider) -> &'static str {
    match provider {
        Provider::AwsS3 => "AWS S3",
        Provider::AzureBlob => "Azure Blob",
        Provider::GcpStorage => "GCP Storage",
        Provider::DigitalOceanSpaces => "DigitalOcean Spaces",
        Provider::LinodeObjStorage => "Linode Object Storage",
        Provider::Unknown => "Unknown",
    }
}

fn check_read(client: &Client, t: &BucketTarget) -> bool {
    let url = construct_read_url(t);
    let resp = client.get(&url).send();
    match resp {
        Ok(r) => match t.provider {
            Provider::AwsS3 | Provider::DigitalOceanSpaces | Provider::LinodeObjStorage =>
                r.status().is_success() && r.text().map_or(false, |body| body.contains("<ListBucketResult")),
            Provider::AzureBlob =>
                r.status().is_success() && r.text().map_or(false, |body| body.contains("EnumerationResults")),
            Provider::GcpStorage =>
                r.status().is_success() && r.text().map_or(false, |body| body.contains("ListBucketResult") || body.contains("xml")),
            _ => false,
        },
        Err(_) => false,
    }
}

fn check_write(client: &Client, t: &BucketTarget) -> bool {
    let url = construct_write_url(t);
    let test_content = b"CodeCompanion write test";
    let resp = client.put(&url).body(test_content.as_ref()).send();
    match resp {
        Ok(r) => r.status().is_success(),
        Err(_) => false,
    }
}

fn main() {
    print_banner();
    let args = Args::parse();

    // Read lines from file if specified, otherwise from stdin
    let lines: Vec<String> = if let Some(input_file) = args.input {
        let file = File::open(&input_file)
            .unwrap_or_else(|e| panic!("Error opening file {}: {}", input_file, e));
        BufReader::new(file)
            .lines()
            .filter_map(|line| line.ok())
            .filter(|line| !line.trim().is_empty())
            .collect()
    } else {
        let stdin = io::stdin();
        stdin
            .lock()
            .lines()
            .filter_map(|line| line.ok())
            .filter(|line| !line.trim().is_empty())
            .collect()
    };

    let client = Client::new();
    let results: Vec<_> = lines.par_iter().map(|line| {
        if let Some(target) = extract_target(line) {
            let readable = check_read(&client, &target);
            let writable = check_write(&client, &target);
            let status = match (readable, writable) {
                (true, true) => "\x1b[32m[read] [write]\x1b[0m",
                (true, false) => "\x1b[32m[read]\x1b[0m",
                (false, true) => "\x1b[33m[write]\x1b[0m",
                (false, false) => "\x1b[31m[no access]\x1b[0m",
            };
            println!("{} | {} | {}", provider_str(&target.provider), target.bucket, status);
            Some((target.provider, target.bucket, readable, writable))
        } else {
            None
        }
    }).filter_map(|r| r).collect();

    // Print summary
    println!("\n\x1b[1;34mBuckets with open permissions:\x1b[0m");
    for (provider, bucket, readable, writable) in &results {
        if *readable || *writable {
            let mut perms = vec![];
            if *readable { perms.push("read"); }
            if *writable { perms.push("write"); }
            println!(
                "{} | {} | [{}]",
                provider_str(provider),
                bucket,
                perms.join(", ")
            );
        }
    }
}
