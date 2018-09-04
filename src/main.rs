extern crate tokio;
extern crate futures;
extern crate tsc;
extern crate byteorder;

use futures::future::{Future};
use futures::stream::Stream;

use tokio::runtime::current_thread;
use tokio::net::{TcpListener, TcpStream};

use tokio::io::read_exact;
use tokio::io::write_all;
use tokio::prelude::future::{loop_fn, Loop};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::io::Cursor;

use futures::sync::mpsc;

use std::thread;

fn main() {
	let mut rt = current_thread::Runtime::new().unwrap();
	let (tx, rx) = mpsc::channel(1024 * 1024 * 1024);
	rt.spawn(server(tx));
	thread::spawn(|| client_thread(1));
	thread::spawn(|| client_thread(2));
	thread::spawn(|| {
		let mut rt = current_thread::Runtime::new().unwrap();
		rt.block_on(in_order_check(rx));
	});
	rt.run();	
}

fn client_thread(id: u64) {
	let mut rt = current_thread::Runtime::new().unwrap();
	println!("starting client {}", id);
	rt.block_on(client(id));
}

#[derive(Debug)]
struct Message {
	sent_at: u64,
	received_at: u64,
	client_id: u64,
}

fn in_order_check(rx: mpsc::Receiver<Message>) -> impl Future<Item = (), Error = ()> {
	println!("starting checker");
	rx.fold(Message { sent_at: 0, received_at: 0, client_id: 0 }, |old, new| {
		assert!(new.received_at > old.received_at);
		if new.sent_at < old.sent_at {
			println!("{:?} sent earlier than {:?}, but received later", new, old);
		}
		//assert!(new.sent_at > old.sent_at);
		//println!("{}: tid {}", new.1, new.0);
		Ok(new)
	}).map(|_| ())
}

fn server(tx: mpsc::Sender<Message>) -> impl Future<Item = (), Error = ()> {
	let server = TcpListener::bind(&(([127, 0, 0, 1], 9000).into())).unwrap();
	let serve = server.incoming().map_err(|e| panic!("{}", e)).for_each(move |stream| {
		let process = loop_fn((stream, tx.clone()), |(stream, mut tx)| {
			let buf = [0;16];
			read_exact(stream, buf).and_then(move |(stream, buf)| {
				let mut cur = Cursor::new(&buf);
				let now = tsc::rdtsc();
				let id = cur.read_u64::<LittleEndian>().unwrap();
				let ts = cur.read_u64::<LittleEndian>().unwrap();
				let msg = Message {
					sent_at: ts,
					received_at: now,
					client_id: id,
				};
				if let Err(err) = tx.try_send(msg) {
					panic!("err {}", err);
				}
				//println!("now {} read {} lat {}", now, ts, now - ts);
				Ok(Loop::Continue((stream, tx)))
			}).or_else(move |err| { panic!("{}", err); Ok(Loop::Break(())) })
		});
		tokio::spawn(process)
	});
	serve
}

fn client(id: u64) -> impl Future<Item = (), Error = ()> {
	let conn = TcpStream::connect(&(([127, 0, 0, 1], 9000).into()))
		.and_then(move |stream| loop_fn((stream, 0u64), move |(stream, count)| {
			let buf = {
				let mut buf = [0;16];
				let ts = tsc::rdtsc();
				{
					let mut cur = Cursor::new(&mut buf[..]);
					cur.write_u64::<LittleEndian>(id).unwrap();
					cur.write_u64::<LittleEndian>(ts).unwrap();
				}
				buf
			};
			if count % 100000 == 0 {
				println!("client {} sent: {}", id, count);
			}
			write_all(stream, buf).and_then(move |(stream, _)| Ok(Loop::Continue((stream, count+1))))
	})).map_err(|e| panic!("{}", e));
	conn
}
