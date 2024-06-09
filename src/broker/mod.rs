mod admin;
mod publisher;
mod subscriber;


use std::error::Error;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::{ToSocketAddrs, TcpStream, TcpListener};
use tokio::sync::broadcast;
use tokio::select;


#[async_trait]
pub trait ConnectionHandler {
    async fn handle_connection(stream: TcpStream, addr: SocketAddr, termination_signal_recvr: broadcast::Receiver<()>);
}

pub struct Broker {
    listner: Option<TcpListener>,
    termination_signal_sender: broadcast::Sender<()>,
    termination_signal_recvr: broadcast::Receiver<()>,
}

impl Broker {
    pub fn new() -> Self {
        let (tx, rx) = broadcast::channel(1);
        Self {
            listner: None,
            termination_signal_sender: tx,
            termination_signal_recvr: rx,
        }
    }

    pub async fn bind<A: ToSocketAddrs + Send>(&mut self, addr: A) -> Result<(), Box<dyn Error>> {
        self.listner = Some(TcpListener::bind(addr).await?);

        Ok(())
    }

    /// panic! if called before calling `self.bind` on self
    pub async fn run<T: ConnectionHandler>(&mut self) {
        match self.listner {
            Some(ref listner) => {
                loop {
                    let termination_future = self.termination_signal_recvr.recv();
                    let connection_future = listner.accept();
                    select! {
                        _ = termination_future => {
                            break;
                        }
                        connection = connection_future => {
                            if let Ok((connection, addr)) = connection {
                                let termination_signal_recvr = self.termination_signal_sender.subscribe();

                                tokio::spawn (async move {
                                    T::handle_connection(connection, addr, termination_signal_recvr).await
                                });
                            }
                        }
                    }
                }
                
            },
            None => panic!("called `self.run()` before calling self.bind on self"),
        }
    }

    pub async fn get_termination_signal_sender(&self) -> broadcast::Sender<()> {
        self.termination_signal_sender.clone()
    }
}

