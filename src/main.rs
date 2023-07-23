use astro_dnssd::DNSServiceBuilder;
use chrono::Utc;
use coap::Server;
use coap_lite::{MessageClass, RequestType as Method};
use futures::prelude::*;
use influxdb2::{Client};
use influxdb2_derive::WriteDataPoint;
use local_ip_address::{local_ip, local_ipv6};

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};


#[derive(Default, WriteDataPoint, Clone, Debug, PartialEq)]
#[measurement = "gassentinel"]
struct GasSentinelDataPoint {
    #[influxdb(tag)]
    device_eui64: String,
    #[influxdb(field)]
    temp: f64,
    #[influxdb(field)]
    hum: f64,
    #[influxdb(field)]
    pres: f64,
    #[influxdb(field)]
    cl1: f64,
    #[influxdb(field)]
    cl2: f64,
    #[influxdb(field)]
    rssi: f64,
    #[influxdb(field)]
    vbat: f64,
    #[influxdb(timestamp)]
    time: i64,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let pattern = r"^([a-fA-F0-9]{16})(,-?[0-9]+){8}$"; // regex pattern for expected payload

    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        println!("Usage: {} <host> <org> <token> <bucket>", args[0]);
        std::process::exit(1);
    }

    let self_ip: Option<Ipv4Addr> = match local_ip().unwrap() {
        IpAddr::V4(ipv4) => Some(ipv4),
        _ => None,
    };
    let self_ipv4 = self_ip.expect("No IPv4 address found!");
    let self_ip6: Option<Ipv6Addr> = match local_ipv6().unwrap() {
        IpAddr::V6(ipv6) => Some(ipv6),
        _ => None,
    };
    let self_ipv6 = self_ip6.expect("No IPv6 address found!");
    println!(
        "Binding to local addresses IPv4: {}, IPv6: {}",
        self_ipv4, self_ipv6
    );
    if self_ip6 == None {
        println!("No IPv6 address found! Unable to continue, exiting.");
        std::process::exit(1);
    }
    let mut coap_server = Server::new(self_ipv6.to_string() + ":5682").unwrap();
    println!("CoAP server up on {}", self_ipv4);


    tokio::spawn(async {
        let service = DNSServiceBuilder::new("_coap._udp", 8080)
            .with_key_value("status".into(), "open".into())
            .register();
        match service {
            Ok(service) => {
                println!("Service registered: {:?}", service);
                std::thread::park();
            }
            Err(e) => {
                println!("Failed to register service: {:?}", e);
            }
        }
    });

    let host = get_argument(&args, 1);
    let org = get_argument(&args, 2);
    let token = get_argument(&args, 3);
    let bucket = Arc::new(Mutex::new(args[4].clone()));
    let client = Arc::new(Mutex::new(Client::new(host, org, token)));

    coap_server
        .run(move |request| {
            // Clone the Arc inside the closure
            let client = Arc::clone(&client);
            let bucket = Arc::clone(&bucket);

            async move {
                let re = regex::Regex::new(pattern).unwrap();
                let payload = String::from_utf8(request.message.payload.clone()).unwrap();
                if re.is_match(&payload[..]) && request.get_method() == &Method::Put {
                    let points = vec![GasSentinelDataPoint {
                        device_eui64: payload[..16].to_string(),
                        temp: payload[17..].split(',').nth(1).unwrap().parse().unwrap(),
                        hum: payload[17..].split(',').nth(2).unwrap().parse().unwrap(),
                        pres: payload[17..].split(',').nth(3).unwrap().parse().unwrap(),

                        cl1: payload[17..].split(',').nth(4).unwrap().parse().unwrap(),
                        cl2: payload[17..].split(',').nth(5).unwrap().parse().unwrap(),
                        rssi: payload[17..].split(',').nth(6).unwrap().parse().unwrap(),
                        vbat: payload[17..].split(',').nth(7).unwrap().parse().unwrap(),
                        time: Utc::now().timestamp_nanos(),
                    }];
                    let client = client.lock().unwrap(); // Acquire the lock
                   
                    let bucket = bucket.lock().unwrap();
                    client.write(&*bucket, stream::iter(points)).await.unwrap();
                    println!("[{}] CoAP payload valid, sent to InfluxDB.", Utc::now().time())
                } else {
                    println!("[{}] CoAP payload malformed.", Utc::now().time());
                    return match request.response {
                        Some(mut message) => {
                            message.message.payload = b"0".to_vec();
                            message.message.header.code =
                                MessageClass::Response(coap_lite::ResponseType::BadOption);
                            Some(message)
                        }
                        _ => None,
                    };
                }

                return match request.response {
                    Some(mut message) => {
                        message.message.payload = b"".to_vec();
                        message.message.header.code =
                            MessageClass::Response(coap_lite::ResponseType::Valid);
                        Some(message)
                    }
                    _ => None,
                };
            }
        })
        .await
        .unwrap();


    Ok(())
}

fn get_argument(args: &[String], index: usize) -> String {
    match args.get(index) {
        Some(arg) => arg.to_string(),
        None => {
            std::process::exit(-1);
        }
    }
}
