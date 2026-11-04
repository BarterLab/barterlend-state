//! Share only requests admitted to a group BEFORE its first HTTP dispatch.
//! Closing checks cannot join an already dispatched opening check. No TTL cache.
use super::*;
use std::{collections::BTreeMap, sync::{Weak, atomic::AtomicBool}};
use tokio::sync::OnceCell;

#[derive(Default)]
pub(super) struct PendingReads(Mutex<BTreeMap<(usize, String, String), Weak<Group>>>);
#[derive(Default)]
struct Group {
    sealed: AtomicBool,
    value: OnceCell<std::result::Result<Value, Failure>>,
}
// RpcClient never produces storage/JSON-persistence errors. Preserve every wire
// error category and RPC code so sharing does not turn permanent failures into retries.
#[derive(Clone)]
enum Failure { Rpc(i64,String), Transport(String), Decode(String), Config(String), Limit(String), Identity(String) }
impl Failure {
    fn from_error(error: Error) -> Self {
        match error {
            Error::Rpc{code,message}=>Self::Rpc(code,message), Error::Transport(s)=>Self::Transport(s),
            Error::Decode(s)=>Self::Decode(s), Error::InvalidConfig(s)=>Self::Config(s),
            Error::ResourceLimit(s)=>Self::Limit(s), Error::IdentityMismatch(s)=>Self::Identity(s),
            _=>Self::Decode("unexpected non-transport error in shared RPC".into()),
        }
    }
    fn error(self)->Error { match self {
        Self::Rpc(code,message)=>Error::Rpc{code,message}, Self::Transport(s)=>Error::Transport(s),
        Self::Decode(s)=>Error::Decode(s), Self::Config(s)=>Error::InvalidConfig(s),
        Self::Limit(s)=>Error::ResourceLimit(s), Self::Identity(s)=>Error::IdentityMismatch(s),
    } }
}
impl PendingReads {
    pub(super) async fn request(&self, rpc:&RpcClient, method:&str, params:Value)->Result<Value> {
        let endpoint=rpc.endpoint_index();
        let key=(endpoint,method.to_owned(),params.to_string());
        let group={
            let mut pending=self.0.lock().await;
            pending.retain(|_,weak|weak.upgrade().is_some_and(|g|!g.sealed.load(Ordering::Acquire)));
            if let Some(group)=pending.get(&key).and_then(Weak::upgrade) {group}
            else {
                // Overflow keeps the ordinary bounded transport path.
                if pending.len()>=256 {drop(pending);return rpc.request_unshared(method,params,None).await;}
                let group=Arc::new(Group::default());pending.insert(key,Arc::downgrade(&group));group
            }
        };
        let result=group.value.get_or_init(||async {
            // Let concurrently queued callers join even when the rate budget
            // is immediately available. Seal before dispatch below; later
            // closing checks still cannot reuse an in-flight/completed read.
            tokio::task::yield_now().await;
            rpc.request_unshared(method,params,Some((&group.sealed,endpoint))).await.map_err(Failure::from_error)
        }).await;
        if rpc.endpoint_index()!=endpoint {return Err(Error::IdentityMismatch("endpoint changed during shared read".into()));}
        result.clone().map_err(Failure::error)
    }
}
