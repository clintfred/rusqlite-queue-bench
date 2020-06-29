use procfs::process::Process;
use rand::Rng;
use rand::RngCore;
use rusqlite::{params, Connection, Result};
use serde::*;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    db_test().await
    // gen_input().await
}

#[derive(Serialize, Deserialize)]
struct StatRec {
    time: u64,
    virtual_memory_bytes: u64,
    resident_memory_bytes: u64,
    shared_memory_bytes: u64,
    db_file_size_bytes: u64,
    inserts_count: u64,
    deletes_count: u64,
    insert_rate: f64,
    delete_rate: f64,
    generated_event_count: u64,
}

fn setup_csv_writer() -> csv::Writer<std::fs::File> {
    let writer = csv::WriterBuilder::new()
        .from_path("out.txt")
        .expect("couldn't open output file");
    writer
}

async fn db_test() -> std::result::Result<(), Box<dyn Error>> {
    // counters for db operations
    let insert_count = Arc::new(AtomicU64::new(0));
    let delete_count = Arc::new(AtomicU64::new(0));

    // counter for events generated
    let generated_count = Arc::new(AtomicU64::new(0));

    let writer = setup_csv_writer();

    let (mut feeder_tx, feeder_rx) = mpsc::channel(3000);

    // TODO this channel either needs to be unbounded or we need to use separate channels
    // for ACKs and figure out how to keep deadlock from happening
    let (db_tx, db_rx) = mpsc::unbounded_channel();

    let socket_to_db = db_tx.clone();
    let sink_to_db = db_tx.clone();

    // THREAD: main thread pulling off ZMQ PULL socket (MAIN)
    // PURPOSE: decode bytes, create DbEvent, Send DbEvent to DB
    // IN: from SOCKET - raw bytes
    // OUT: to DB - DbEvent with id, decoded and raw bytes
    let socket_read_thread = create_read_socket_thread(feeder_rx, socket_to_db);

    let (log_sink_tx, log_sink_rx) = mpsc::channel(5000);

    let conn = Connection::open("./test.db")?;
    if let Err(_e) = create_rustqlite_table(&conn) {
        println!("Skipping db creation... test.db already exists!")
    }

    // THREAD: DatabaseWriter (DW)
    // PURPOSE: Broker for database interactions. Insert and delete.
    // IN:  from MAIN - Insert DbEvents -
    //      from SINK - Delete DbEvents - after SINK confirms receipt of event in external system, delete event out of storage.
    // OUT: to SINK   - send recorded events to external sink
    let _db_thread = create_db_thread(
        db_rx,
        log_sink_tx,
        insert_count.clone(),
        delete_count.clone(),
        conn,
    );

    // THREAD: Log Sink (SINK)
    // PURPOSE: send out log messages to an external system. Get confirmation of recript and signal DW.
    // IN: from DW - message to be sent out
    // OUT: to DW  - delete message (successfully sent out)
    let _sink_thread = create_log_sink_thread(sink_to_db, log_sink_rx);

    // THREAD: Periodic stats gathering
    let _stats_timer = start_stats_timer(
        generated_count.clone(),
        insert_count.clone(),
        delete_count.clone(),
        writer,
    );

    //MAIN THREAD: emulate the ZMQ PULL socket (SOCKET)

    // note that this the feeder_tx channel needs to be able to hold this many messages
    let msgs_per_sec_target = 1000u64;
    let mut start = Instant::now();
    loop {
        let file2 = File::open("input100k.txt").await?;
        let reader = BufReader::new(file2);
        let mut stream = reader.lines();
        while let Some(l) = stream
            .next_line()
            .await
            .expect("reading from cursor won't fail")
        {
            let i = generated_count.fetch_add(1, Ordering::Relaxed);
            let ie: InputEvent = serde_json::from_str(&l)?;
            let ie_bytes = bincode::serialize(&ie)?;
            if i % 10000 == 0 {
                println!("Generating logging event -  id {}, #{}", &ie.ray_id, &i);
            }

            if let Err(e) = feeder_tx.send(ChannelMsg::Msg(Bytes(ie_bytes))).await {
                println!("error sending generated data to SOCKET READ thread: {}", e);
            }

            // after we have put out as many messages as we want to in 1 second
            // calculate how long we need to wait to start generating the next
            // second's worth of messages, and then wait for that duration
            if i % msgs_per_sec_target == 0 {
                let end = Instant::now();
                let elapsed = end - start;
                let time_until_next_iter = Duration::from_millis(1000) - elapsed;
                // println!("delaying for {:?}", &time_until_next_iter);
                tokio::time::delay_for(time_until_next_iter).await;
                start = Instant::now(); // start time of the new "second"
            }
        }
    }

    // GRACEFUL SHUTDOWN DISABLED FOR NOW
    // if let Err(e) = feeder_tx.send(ChannelMsg::Poison).await {
    //     println!("error sending POISON data to SOCKET READ thread: {}", e);
    // }
    // tokio::join!(sink_thread, socket_read_thread, db_thread);

    // Ok(())
}

fn start_stats_timer(
    generated_count: Arc<AtomicU64>,
    insert_count: Arc<AtomicU64>,
    delete_count: Arc<AtomicU64>,
    mut writer: csv::Writer<std::fs::File>,
) -> JoinHandle<()> {
    let tick_time_ms = 60000;
    let tick_time_sec = tick_time_ms as f64 / 1000f64;

    tokio::spawn(async move {
        let me = Process::myself().expect("unable to load self process");
        let db_file = File::open("./test.db")
            .await
            .expect("unable to open db file");

        let mut interval = tokio::time::interval(Duration::from_millis(tick_time_ms));

        let mut i = 0u64;
        let mut inserts_prev = 0f64;
        let mut deletes_prev = 0f64;
        loop {
            interval.tick().await;
            i += 1;

            let inserts = insert_count.load(Ordering::Relaxed) as f64;
            let insert_rate = (inserts - inserts_prev) / tick_time_sec;
            inserts_prev = inserts;

            let deletes = delete_count.load(Ordering::Relaxed) as f64;
            let delete_rate = (deletes - deletes_prev) / tick_time_sec;
            deletes_prev = deletes;

            print_mem(&me);

            let (vm, rss, shared) = if let Ok(status) = me.status() {
                (
                    status.vmsize.expect("vmsize") * 1024,
                    status.vmrss.expect("vmrss") * 1024,
                    status.rssfile.expect("rssfile") * 1024
                        + status.rssshmem.expect("rssshmem") * 1024,
                )
            } else {
                (0, 0, 0)
            };

            let stats = StatRec {
                time: i * tick_time_sec as u64,
                virtual_memory_bytes: vm,
                resident_memory_bytes: rss,
                shared_memory_bytes: shared,
                db_file_size_bytes: db_file
                    .metadata()
                    .await
                    .expect("failed to get db file size")
                    .len(),
                inserts_count: inserts as u64,
                deletes_count: deletes as u64,
                insert_rate: insert_rate,
                delete_rate: delete_rate,
                generated_event_count: generated_count.load(Ordering::Relaxed),
            };

            writer.serialize(stats);
            writer.flush();

            println!("DB file size: {}", db_file.metadata().await.unwrap().len());

            println!("Inserts: {}", &inserts);
            println!("Inserts/sec: {}", &insert_rate);
            println!("Deletes: {}", &deletes);
            println!("Deletes/sec: {}", &delete_rate);
        }
    })
}

fn create_log_sink_thread(
    sink_to_db: UnboundedSender<DbAction>,
    mut log_sink_rx: Receiver<ChannelMsg<LogEvent>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        println!("starting sink thread...");
        while let Some(ChannelMsg::Msg(le)) = log_sink_rx.recv().await {
            // println!("LOG SINK - SENDING {} to Stackdriver", &le.ray_id[0..5]);
            // std::thread::sleep(Duration::from_millis(250));
            if let Err(e) = sink_to_db.send(DbAction::Delete(le.ray_id)) {
                println!("LOG SINK - error sending ACK to db thread: {}", e);
            }
        }

        println!("LOG SINK - Exiting");
    })
}

fn print_mem(me: &Process) {
    // let page_size = procfs::page_size().expect("Unable to determinte page size!") as u64;
    // if let Ok(statm) = me.statm() {
    //     println!("== Data from /proc/self/statm:");
    //     println!(
    //         "Total virtual memory used: {} pages ({} bytes)",
    //         statm.size,
    //         statm.size * page_size
    //     );
    //     println!(
    //         "Total resident set: {} pages ({} byte)s",
    //         statm.resident,
    //         statm.resident * page_size
    //     );
    //     println!(
    //         "Total shared memory: {} pages ({} bytes)",
    //         statm.shared,
    //         statm.shared * page_size
    //     );
    //     println!();
    // }

    if let Ok(status) = me.status() {
        println!("== Data from /proc/self/status:");
        println!(
            "Total virtual memory used: {} bytes",
            status.vmsize.expect("vmsize") * 1024
        );
        println!(
            "Total resident set: {} bytes",
            status.vmrss.expect("vmrss") * 1024
        );
        println!(
            "Total shared memory: {} bytes",
            status.rssfile.expect("rssfile") * 1024 + status.rssshmem.expect("rssshmem") * 1024
        );
    }
}

fn create_db_thread(
    mut db_rx: UnboundedReceiver<DbAction>,
    mut log_sink_tx: Sender<ChannelMsg<LogEvent>>,
    insert_count: Arc<AtomicU64>,
    delete_count: Arc<AtomicU64>,
    conn: Connection,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start_time = std::time::Instant::now();
        let mut i = 0usize;
        while let Some(db_action) = db_rx.next().await {
            match db_action {
                DbAction::Poison => {
                    println!(
                        "DB - POISON EVENT received. Waiting for final ACK to close rx channel..."
                    );
                    println!("DB - SENDING POISON to LOG SINK...");
                    if let Err(_e) = log_sink_tx.send(ChannelMsg::Poison).await {
                        println!("DB - Failed to send POISON to LOG SINK");
                        db_rx.close();
                    } else {
                        //TODO this logic isn't right and will result in acks being dropped
                        println!("DB - WAITING for final ACK from LOG SINK...");
                        if let Some(x) = db_rx.recv().await {
                            println!("DB - recv final ACK: {:?}", x);
                        }
                        println!("DB - ACK RECEIVED. Closing db_rx...");
                        db_rx.close();
                    }
                }
                DbAction::Insert(le) => {
                    insert_count.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                    if i % 100 == 0 {
                        // println!("DB - processing event {} - #{}", le.ray_id, &i);
                    }
                    conn.execute(
                        "INSERT INTO LOG_EVENTS (ray_id, data) VALUES (?1, ?2)",
                        params![le.ray_id, le.data],
                    )
                    .unwrap();
                    if let Err(e) = log_sink_tx.send(ChannelMsg::Msg(le)).await {
                        println!("Log message not sent to LOG SINK thread: {}", e);
                    }
                }
                DbAction::Delete(ray_id) => {
                    delete_count.fetch_add(1, Ordering::Relaxed);
                    conn.execute("DELETE from LOG_EVENTS where ray_id = ?1", params![ray_id])
                        .unwrap();
                    i += 1;
                    if i % 100 == 0 {
                        // println!("DB - ACK {} - #{}", &ray_id, &i);
                    }
                } //println!("DB - deleting entry for ray_id {}", ray_id)
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time - start_time;
        println!("Test completed in {:?}", &duration);

        // let mut stmt = conn
        //     .prepare("SELECT id, ray_id, data FROM LOG_EVENTS")
        //     .unwrap();
        // let events = stmt
        //     .query_map(params![], |row| {
        //         Ok(LogEvent {
        //             id: row.get(0).unwrap(),
        //             ray_id: row.get(1).unwrap(),
        //             plaintext: Plaintext::new_from_slice(
        //                 row.get::<_, Vec<u8>>(2).unwrap().as_ref(),
        //             )
        //             .unwrap(),
        //             data: None,
        //         })
        //     })
        //     .unwrap();
        //
        // // println!("ENTRIES IN DB: {}", &events.collect::<Vec<_>>().len());
        //
        // for e in events {
        //     println!("Found event {:?}", e.unwrap());
        // }
    })
}

fn create_read_socket_thread(
    mut feeder_rx: Receiver<ChannelMsg<Bytes>>,
    socket_to_db: UnboundedSender<DbAction>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ChannelMsg::Msg(Bytes(socket_bytes))) = feeder_rx.recv().await {
            let event = LogEvent {
                id: 0,
                ray_id: create_ray_id(),
                data: Some(socket_bytes),
            };
            if let Err(_) = socket_to_db.send(DbAction::Insert(event)) {
                println!("receiver dropped");
                return;
            }
        }

        println!("SOCKET - POISON received. Closing channel rx...");
        feeder_rx.close();
        println!("SOCKET - Sending POISON to DB thread...");

        if let Err(e) = socket_to_db.send(DbAction::Poison) {
            println!("error sending POISON to DB thread: {}", e);
        }
    })
}

fn create_rustqlite_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE LOG_EVENTS (
              _id             INTEGER PRIMARY KEY AUTOINCREMENT,
              ray_id          TEXT NOT NULL,
              created_ms      DATETIME DEFAULT current_timestamp,
              send_attempts   INTEGER default 0,
              data            BLOB,
              UNIQUE(ray_id, created_ms)
              )",
        params![],
    );

    // let journal_mode =
    conn.pragma_update(None, "journal_mode", &"WAL".to_string())?;
    // dbg!(&journal_mode);
    Ok(())
}

pub async fn gen_input() -> std::result::Result<(), Box<dyn Error>> {
    let mut file = File::create("input.txt").await?;

    for _i in 0..10000usize {
        let msg_len = rand::thread_rng().gen_range(250, 2500);
        let mut msg = vec![0u8; msg_len];
        rand::thread_rng().fill_bytes(&mut msg);
        let e = InputEvent::with_msg(&base64::encode(&msg));
        file.write_all(&serde_json::to_vec(&e)?).await?;
        file.write_all(b"\n").await?;
    }

    file.sync_all().await?;

    let file2 = File::open("input.txt").await?;
    let reader = BufReader::new(file2);
    let mut stream = reader.lines();

    let mut i = 0usize;
    while let Some(l) = stream
        .next_line()
        .await
        .expect("reading from cursor won't fail")
    {
        let ie: InputEvent = serde_json::from_str(&l)?;
        if i % 1000 == 0 {
            dbg!(&ie.ray_id);
        }
        i += 1;
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct LogEvent {
    id: i64,
    ray_id: String,
    data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct InputEvent {
    ray_id: String,
    log_msg: String,
}

impl InputEvent {
    fn with_msg(m: &str) -> InputEvent {
        InputEvent {
            ray_id: create_ray_id(),
            log_msg: m.to_string(),
        }
    }
}

enum ChannelMsg<M> {
    Msg(M),
    Poison,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DbAction {
    Insert(LogEvent),
    Delete(String),
    Poison,
}

fn create_ray_id() -> String {
    base64::encode_config(
        rand::thread_rng().gen::<[u8; 12]>(),
        base64::URL_SAFE_NO_PAD,
    )
}

struct Bytes(Vec<u8>);
