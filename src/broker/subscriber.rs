use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::sync::broadcast;


use super::ConnectionHandler;

pub struct SimpleSubscriberConnectionHandler;


#[async_trait]
impl ConnectionHandler for SimpleSubscriberConnectionHandler {
    async fn handle_connection(stream: TcpStream, addr: SocketAddr, termination_signal_recvr: broadcast::Receiver<()>) {
    }
}

