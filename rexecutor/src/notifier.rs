use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::{mpsc, OnceCell, RwLock};

use crate::WakeMessage;

#[async_trait]
pub(crate) trait Notifier {
    async fn enroll(name: &'static str, notifier: Box<dyn Notify>);
    fn notify(name: &'static str);
}

pub(crate) trait Notify: Send + Sync {
    fn notify(&self);
}

impl Notify for mpsc::UnboundedSender<WakeMessage> {
    fn notify(&self) {
        if let Err(err) = self.send(WakeMessage::Wake) {
            tracing::error!(?err, "Failed to send executor wake message")
        }
    }
}

static EXECUTORS: OnceCell<RwLock<HashMap<&'static str, Box<dyn Notify>>>> = OnceCell::const_new();

pub(crate) struct InProcessNotifier;

#[async_trait]
impl Notifier for InProcessNotifier {
    async fn enroll(name: &'static str, notifier: Box<dyn Notify>) {
        EXECUTORS
            .get_or_init(|| async { Default::default() })
            .await
            .write()
            .await
            .insert(name, notifier);
    }

    fn notify(name: &'static str) {
        EXECUTORS.get().iter().for_each(|inner| {
            tokio::spawn(async {
                inner
                    .read()
                    .await
                    .get(name)
                    .map(|notifier| notifier.notify())
            });
        });
    }
}
