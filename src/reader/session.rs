//! Task-local verification, never a wall-clock cache. No data survives a scope.
use super::*;
use std::{cell::RefCell, future::Future};

tokio::task_local! { static SESSION: RefCell<Session>; }
#[derive(Default)]
struct Session {
    headers: BTreeMap<(usize,usize,u64),(HttpReader,BlockRef)>,
    identities: BTreeMap<(usize,usize),(HttpReader,ChainIdentity)>,
    codes: BTreeMap<(usize,usize,Address,B256),Bytes>,
}
const MAX_HEADERS: usize = 128;
pub(super) fn code(reader:&HttpReader,address:Address,hash:B256)->Option<Bytes> {
    let (id,endpoint)=reader.rpc.session_key();
    SESSION.try_with(|s|s.borrow().codes.get(&(id,endpoint,address,hash)).cloned()).ok().flatten()
}
pub(super) fn remember_code(reader:&HttpReader,address:Address,hash:B256,code:Bytes) {
    let (id,endpoint)=reader.rpc.session_key();
    let _=SESSION.try_with(|s|{let mut s=s.borrow_mut();if s.codes.len()<MAX_HEADERS {s.codes.insert((id,endpoint,address,hash),code);}});
}

pub(super) fn identity(reader: &HttpReader) -> Option<ChainIdentity> {
    SESSION.try_with(|s| s.borrow().identities.get(&reader.rpc.session_key()).map(|(_,id)| *id)).ok().flatten()
}
pub(super) fn remember_identity(reader: &HttpReader, identity: ChainIdentity) {
    let _ = SESSION.try_with(|s| {
        let mut s=s.borrow_mut();
        if s.identities.len()<MAX_HEADERS { s.identities.insert(reader.rpc.session_key(),(reader.clone(),identity)); }
    });
}

pub(super) fn cached(reader: &HttpReader, number: u64) -> Option<BlockRef> {
    let (id, endpoint) = reader.rpc.session_key();
    SESSION.try_with(|scope| scope.borrow().headers.get(&(id,endpoint,number)).map(|(_,at)| *at)).ok().flatten()
}
pub(super) fn remember(reader: &HttpReader, at: BlockRef) -> Result<()> {
    SESSION.try_with(|scope| {
        let mut scope = scope.borrow_mut();
        let (id,endpoint) = reader.rpc.session_key();
        let key = (id,endpoint,at.number);
        if scope.headers.get(&key).is_some_and(|(_,old)|old.hash!=at.hash || old.parent_hash!=at.parent_hash || old.timestamp!=at.timestamp) {
            return Err(Error::NonCanonical(at.number));
        }
        if scope.headers.len() >= MAX_HEADERS && !scope.headers.contains_key(&key) {
            return Ok(()); // Overflow reads retain their ordinary fresh checks.
        }
        scope.headers.insert(key,(reader.clone(),at));
        Ok(())
    }).unwrap_or(Ok(()))
}

// Internal engine scope: its owner rolls back staged publication/progress on
// error or cancellation. Nested scopes are prohibited. Outside this scope every
// public reader operation retains its ordinary before/after canonical bracket.
pub(crate) async fn verified_session<T>(future: impl Future<Output=Result<T>>) -> Result<T> {
    if SESSION.try_with(|_| ()).is_ok() { return Err(Error::InvalidConfig("nested verified read session".into())); }
    SESSION.scope(RefCell::new(Session::default()), async {
        let result = future.await;
        let Session { headers, identities, .. } = SESSION.with(|scope| std::mem::take(&mut *scope.borrow_mut()));
        // Always bypass memoization here, including after a read failure.
        for ((_,endpoint,_),(reader,at)) in headers {
            if reader.rpc.session_key().1 != endpoint {
                return Err(Error::IdentityMismatch("endpoint changed during read session".into()));
            }
            let current = reader.uncached_block(at.number,at.commitment).await?.ok_or(Error::MissingBlock(at.number))?;
            if reader.rpc.session_key().1 != endpoint { return Err(Error::IdentityMismatch("endpoint changed during closing read".into())); }
            if current != at { return Err(Error::NonCanonical(at.number)); }
        }
        for ((_,endpoint),(reader,identity)) in identities {
            if reader.rpc.session_key().1 != endpoint { return Err(Error::IdentityMismatch("endpoint changed during read session".into())); }
            let chain=reader.rpc.request("eth_chainId",json!([])).await?;
            if reader.rpc.session_key().1 != endpoint { return Err(Error::IdentityMismatch("endpoint changed during closing identity".into())); }
            if parse_quantity_value(&chain,"chainId")? != identity.chain_id {
                return Err(Error::IdentityMismatch("chain changed during read session".into()));
            }
        }
        result
    }).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::{BufRead,BufReader,Read,Write},net::TcpListener,sync::atomic::{AtomicBool,AtomicUsize,Ordering}};
    use std::time::Duration;
    fn server() -> (String,Arc<AtomicUsize>,Arc<AtomicBool>,Arc<AtomicBool>,std::thread::JoinHandle<()>) {
        let listener=TcpListener::bind("127.0.0.1:0").unwrap(); listener.set_nonblocking(true).unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let count=Arc::new(AtomicUsize::new(0)); let reorg=Arc::new(AtomicBool::new(false)); let stop=Arc::new(AtomicBool::new(false));
        let (n,r,s)=(count.clone(),reorg.clone(),stop.clone());
        let thread=std::thread::spawn(move||while !s.load(Ordering::SeqCst) {
            let Ok((mut socket,_))=listener.accept() else {std::thread::sleep(Duration::from_millis(1));continue;};
            socket.set_nonblocking(false).unwrap();socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut input=BufReader::new(socket.try_clone().unwrap());let mut line=String::new();input.read_line(&mut line).unwrap();
            let mut len=0;loop {line.clear();input.read_line(&mut line).unwrap();if line=="\r\n"{break;}
                if let Some(v)=line.to_ascii_lowercase().strip_prefix("content-length:"){len=v.trim().parse().unwrap();}}
            let mut body=vec![0;len];input.read_exact(&mut body).unwrap();let request:Value=serde_json::from_slice(&body).unwrap();
            n.fetch_add(1,Ordering::SeqCst);
            let body=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":{
                "number":"0x7","hash":B256::repeat_byte(if r.load(Ordering::SeqCst){8}else{7}),
                "parentHash":B256::repeat_byte(6),"timestamp":"0x46"}})).unwrap();
            let _=write!(socket,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());let _=socket.write_all(&body);
        });
        (url,count,reorg,stop,thread)
    }
    #[tokio::test]
    async fn repeated_checks_share_only_one_scope_and_final_reorg_is_rejected() {
        let (url,count,reorg,stop,thread)=server();
        let reader=HttpReader::new(&url,RpcConfig{min_request_interval:Duration::ZERO,max_retries:0,..RpcConfig::default()}).unwrap();
        let at=BlockRef{number:7,hash:B256::repeat_byte(7),parent_hash:B256::repeat_byte(6),timestamp:70,commitment:Commitment::Finalized};
        verified_session(async {for _ in 0..20 {reader.ensure_canonical(&at).await?;} Ok(())}).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst),2,"one fresh opening and closing header");
        reader.ensure_canonical(&at).await.unwrap();assert_eq!(count.load(Ordering::SeqCst),3,"nothing cached between scopes");
        let error=verified_session(async {reader.ensure_canonical(&at).await?;reorg.store(true,Ordering::SeqCst);reader.ensure_canonical(&at).await}).await.unwrap_err();
        assert!(matches!(error,Error::NonCanonical(7)));
        reorg.store(false,Ordering::SeqCst);
        let result=tokio::time::timeout(Duration::from_millis(20),verified_session(async {
            reader.ensure_canonical(&at).await?; std::future::pending::<Result<()>>().await
        })).await;
        assert!(result.is_err());let before=count.load(Ordering::SeqCst);
        reader.ensure_canonical(&at).await.unwrap();assert_eq!(count.load(Ordering::SeqCst),before+1,"cancelled scope cannot leak its cache");
        let result=reader.verified_read(async {
            let first=reader.head(Commitment::Latest).await?;
            reorg.store(true,Ordering::SeqCst);
            reader.head(Commitment::Latest).await?;
            Ok::<_,Error>(first)
        }).await;
        assert!(matches!(result,Err(Error::NonCanonical(7))),"a later head must not overwrite earlier evidence at the same height");
        stop.store(true,Ordering::SeqCst);thread.join().unwrap();
    }
}
