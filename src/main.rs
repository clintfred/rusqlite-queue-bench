use futures::prelude::*;
use futures::FutureExt;
use rand::{Rng, RngCore};
use recrypt::api::{Plaintext, RecryptErr};
use recrypt::prelude::*;
use rusqlite::{params, Connection, Result};
use serde::*;
use std::error::Error;
use std::io::{BufRead, LineWriter, Write};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

// #[tokio::main]
async fn main_1() -> std::result::Result<(), Box<dyn Error>> {
    let recrypt = Recrypt::new();
    let recrypt2 = Recrypt::new();

    let (mut feeder_tx, mut feeder_rx) = mpsc::unbounded_channel();

    let (mut db_tx, mut db_rx) = mpsc::channel(100);

    let mut socket_to_db = db_tx.clone();
    let mut sink_to_db = db_tx.clone();

    // THREAD: main thread pulling off ZMQ PULL socket (MAIN)
    // PURPOSE: decode bytes, create DbEvent, Send DbEvent to DB
    // IN: from SOCKET - raw bytes
    // OUT: to DB - DbEvent with id, decoded and raw bytes
    let socket_read_thread = tokio::spawn(async move {
        while let Some(Bytes(socket_bytes)) = feeder_rx.recv().await {
            let plaintext = Plaintext::new_from_slice(&socket_bytes).unwrap();
            let ray_id = base64::encode_config(
                recrypt2.random_private_key().bytes(),
                base64::URL_SAFE_NO_PAD,
            );

            let event = LogEvent {
                id: 0,
                ray_id,
                plaintext,
                data: Some(socket_bytes),
            };
            if let Err(_) = socket_to_db.send(DbAction::Insert(event)).await {
                println!("receiver dropped");
                return;
            }
        }
        socket_to_db.send(DbAction::Poison).await;
    });

    let (mut log_sink_tx, mut log_sink_rx) = mpsc::channel(100);

    let conn = Connection::open("./test.db")?;
    if let Err(e) = create_rustqlite_table(&conn) {
        println!("Skipping db creation... test.db already exists!")
    }

    // THREAD: DatabaseWriter (DW)
    // PURPOSE: Broker for database interactions. Insert and delete.
    // IN:  from MAIN - Insert DbEvents -
    //      from SINK - Delete DbEvents - after SINK confirms receipt of event in external system, delete event out of storage.
    // OUT: to SINK   - send recorded events to external sink
    let db_thread = tokio::spawn(async move {
        let mut closed_senders_count = 0u8;
        while let Some(i) = db_rx.recv().await {
            // println!("got = {:?}", i);

            match i {
                DbAction::Poison => {
                    println!("POISON EVENT");
                    db_rx.close()
                }
                DbAction::Insert(le) => {
                    println!("processing event {}", le.ray_id);
                    conn.execute(
                        "INSERT INTO LOG_EVENTS (ray_id, data) VALUES (?1, ?2)",
                        params![le.ray_id, le.data],
                    )
                    .unwrap();
                    log_sink_tx.send(le).await;
                }
                DbAction::Delete(ray_id) => println!("deleting entry for ray_id {}", ray_id),
            }
        }

        let mut stmt = conn
            .prepare("SELECT id, ray_id, data FROM LOG_EVENTS")
            .unwrap();
        let events = stmt
            .query_map(params![], |row| {
                Ok(LogEvent {
                    id: row.get(0).unwrap(),
                    ray_id: row.get(1).unwrap(),
                    plaintext: Plaintext::new_from_slice(
                        row.get::<_, Vec<u8>>(2).unwrap().as_ref(),
                    )
                    .unwrap(),
                    data: None,
                })
            })
            .unwrap();

        println!("ENTRIES IN DB: {}", events.collect::<Vec<_>>().len());

        // for e in events {
        // println!("Found event {:?}", e.unwrap());
        // }
    });

    // THREAD: Log Sink (SINK)
    // PURPOSE: send out log messages to an external system. Get confirmation of recript and signal DW.
    // IN: from DW - message to be sent out
    // OUT: to DW  - delete message (successfully sent out)
    let sink_thread = tokio::spawn(async move {
        println!("starting sink thread...");
        while let Some(le) = log_sink_rx.recv().await {
            println!("SENDING {} to Stackdriver", &le.ray_id[0..5]);
            // std::thread::sleep(Duration::from_millis(250));
            sink_to_db.send(DbAction::Delete(le.ray_id)).await;

            // tokio::task::yield_now();
        }
    });

    //MAIN THREAD: emulate the ZMQ PULL socket (SOCKET)
    for i in 0..10 {
        let plaintext = recrypt.gen_plaintext();
        println!("generating plaintext: {} - channel size", &i);
        // if let Err(_) =
        feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));
        // feeder_tx.send(Bytes(plaintext.bytes().to_vec()));

        // {
        //     println!("receiver dropped");
        //     return;
        // }
    }

    tokio::join!(sink_thread, socket_read_thread, db_thread);

    Ok(())
}

fn create_rustqlite_table(conn: &Connection) -> Result<usize> {
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
    )
}

#[tokio::main]
pub async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let mut file = File::create("poem.txt").await?;

    for i in 0..100000usize {
        let msg_len = rand::thread_rng().gen_range(250, 2500);
        let mut msg = vec![0u8; msg_len];
        rand::thread_rng().fill_bytes(&mut msg);
        let e = InputEvent::with_msg(&base64::encode(&msg));
        file.write_all(&serde_json::to_vec(&e)?).await?;
        file.write_all(b"\n").await?;
    }

    // file.write_all(&serde_json::to_vec(&e)?).await?;
    // file.write_all(b"\n").await?;
    // file.write_all(b"aasdfasdf\n").await?;
    // file.write_all(b"asdsdfasdf\n").await?;
    // file.write_all(b"asdgfdgfasdf\n").await?;
    // file.write_all(b"asdfasggfdf\n").await?;
    // file.write_all(b"asdfassadfdf\n").await?;
    // file.write_all(b"asdfaasdfsdf\n").await?;
    // file.write_all(b"asdfasdf\n").await?;
    // file.write_all(b"asdfasasdfdf\n").await?;
    // file.write_all(b"asdfasdaf\n").await?;

    file.sync_all().await?;

    let mut file2 = File::open("poem.txt").await?;
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

fn write_to_db() {}
#[derive(Debug, Clone, Eq, PartialEq)]
struct LogEvent {
    id: i64,
    ray_id: String,
    plaintext: Plaintext,
    data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct InputEvent {
    ray_id: String,
    log_msg: String,
}

impl InputEvent {
    fn with_msg(m: &str) -> InputEvent {
        let ray_id = base64::encode_config(
            rand::thread_rng().gen::<[u8; 12]>(),
            base64::URL_SAFE_NO_PAD,
        );
        InputEvent {
            ray_id,
            log_msg: m.to_string(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DbAction {
    Insert(LogEvent),
    Delete(String),
    Poison,
}

struct Bytes(Vec<u8>);
