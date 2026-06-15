// A fast multi-threaded HTTP/1.1 echo server used to benchmark wrk-rs itself.
// Listens on 127.0.0.1:18081, replies 200 OK with a short body. Run with
// `cargo run --release --example bench_server` then point wrk-rs at it.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

const BODY: &[u8] = b"OK\n";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let addr: SocketAddr = "127.0.0.1:18081".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    eprintln!("bench_server listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .timer(TokioTimer::new())
                .keep_alive(true)
                .serve_connection(
                    io,
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(200)
                                .header("Content-Length", BODY.len())
                                .body(Full::new(Bytes::from_static(BODY)))
                                .unwrap(),
                        )
                    }),
                )
                .await
            {
                eprintln!("conn error: {e}");
            }
        });
    }
}
