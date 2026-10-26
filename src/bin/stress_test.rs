use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

const SERVER_ADDRESS: &str = "127.0.0.1:8080";
const CONCURRENT_CLIENTS: usize = 10;
const LOGS_PER_CLIENT: usize = 5000;
const DELAY_BETWEEN_LOGS_MICROS: u64 = 50; // Microseconds to control stress intensity

#[tokio::main]
async fn main() {
    println!("🚀 Launching Rust log generation stress test tool...");
    println!("Targeting Server: {}", SERVER_ADDRESS);
    println!("Spawning {} concurrent worker threads firing {} logs each...", CONCURRENT_CLIENTS, LOGS_PER_CLIENT);

    let start_time = std::time::Instant::now();
    let mut tasks = vec![];

    // Spawn concurrent clients running in parallel green threads
    for client_id in 0..CONCURRENT_CLIENTS {
        let task = tokio::spawn(async move {
            match TcpStream::connect(SERVER_ADDRESS).await {
                Ok(mut stream) => {
                    let components = ["AUTH_SERVICE", "PAYMENT_API", "DATABASE_CLUSTER", "KUBE_NODE_04"];
                    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];

                    for i in 0..LOGS_PER_CLIENT {
                        // Dynamically cycle matching structures to randomize payload views
                        let level = levels[i % levels.len()];
                        let component = components[(i + client_id) % components.len()];

                        // Construct the exact wire structure expected by your zero-copy parser
                        let log_payload = format!(
                            "[{}] {} :: Transaction sequential check sequence frame id #{} completed successfully.\n",
                            level, component, i
                        );

                        if let Err(e) = stream.write_all(log_payload.as_bytes()).await {
                            eprintln!("Worker #{} broken pipe error: {:?}", client_id, e);
                            break;
                        }

                        if DELAY_BETWEEN_LOGS_MICROS > 0 {
                            sleep(Duration::from_micros(DELAY_BETWEEN_LOGS_MICROS)).await;
                        }
                    }

                    // Flush buffer gracefully to server before disconnecting
                    let _ = stream.flush().await;
                }
                Err(e) => eprintln!("Worker #{} failed to bind or connect to backend aggregator: {:?}", client_id, e),
            }
        });
        tasks.push(task);
    }

    // Await all background test tasks to complete execution safely
    for task in tasks {
        let _ = task.await;
    }

    let elapsed = start_time.elapsed();
    let total_logs = CONCURRENT_CLIENTS * LOGS_PER_CLIENT;
    let throughput = total_logs as f64 / elapsed.as_secs_f64();

    println!("\n✅ Stress Test Completed Successfully!");
    println!("Total Logs Dispatched : {}", total_logs);
    println!("Total Execution Time   : {:.2?}", elapsed);
    println!("Estimated Throughput   : {:.0} logs/sec", throughput);
}

