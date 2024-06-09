use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::sync::broadcast;


use super::ConnectionHandler;

pub struct SimplePublisherConnectionHandler;


#[async_trait]
impl ConnectionHandler for SimplePublisherConnectionHandler {
    async fn handle_connection(stream: TcpStream, addr: SocketAddr, termination_signal_recvr: broadcast::Receiver<()>) {
        

    }
}

