//! Real CreditPool operations on an isolated local Anvil.
//! Only the test client below can impersonate and send; the SDK remains read-only.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, Bytes, U256, b256};
use alloy_sol_types::{SolCall, SolEvent, sol};
use barterlend_state::{Checkpoint, Commitment, EngineConfig, HttpReader, OracleFailure,
    OracleStatus, PoolSpec, PositionAmounts, PositionKey, RiskFailure, RiskStatus,
    StateEngine, StateReader, StateSnapshot, SyncOutcome, abi::CreditPool,
    rpc::{BlockPinning, RpcConfig}};
use serde::Deserialize;
use serde_json::{Value, json};

const WAD: u128 = 1_000_000_000_000_000_000;
const GENESIS: B256 = b256!("3e8b48d8cbf5dfa86c941f2c1821786417d34ef516ef1a336fdb035f984f89e9");

sol! {
    interface FixtureToken {
        function mint(uint256 amount) external;
        function approve(address spender, uint256 amount) external returns (bool);
    }
    interface FixtureOracle {
        function setPrice(uint256 newPrice) external;
    }
}

#[derive(Deserialize)]
struct Addresses {
    pool: Address,
    asset: Address,
    collateral: Address,
    oracle: Address,
    admin: Address,
    provider_a: Address,
    provider_b: Address,
    provider_c: Address,
    borrower_a: Address,
    borrower_b: Address,
    market0: String,
    market1: String,
}

/// Does not use the SDK RpcClient: its allowlist excludes send methods.
struct LocalTransactions {
    client: reqwest::Client,
    url: url::Url,
    next_timestamp: u64,
}

impl LocalTransactions {
    async fn checked(url: &str, addresses: &Addresses) -> Self {
        let url = url::Url::parse(url).unwrap();
        assert_eq!(url.scheme(), "http", "local test only");
        assert_eq!(url.host_str(), Some("127.0.0.1"), "local test only");
        assert!(url.port().is_some_and(|port| port != 0));
        assert!(url.username().is_empty() && url.password().is_none());
        assert!(url.query().is_none() && url.fragment().is_none() && url.path() == "/");
        let mut local = Self {
            client: reqwest::Client::builder().no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(5)).build().unwrap(),
            url, next_timestamp: 2,
        };
        assert_eq!(local.rpc("eth_chainId", json!([])).await, json!("0x7a69"));
        let genesis = local.rpc("eth_getBlockByNumber", json!(["0x0", false])).await;
        assert_eq!(serde_json::from_value::<B256>(genesis["hash"].clone()).unwrap(), GENESIS);
        assert_eq!(genesis["timestamp"], "0x1");
        // This test must start before any transactions, never join a shared node.
        assert_eq!(local.rpc("eth_blockNumber", json!([])).await, json!("0x0"));
        let state: Value = serde_json::from_str(include_str!(
            "fixtures/protocol-v5/generated/empty-pool-state.json"
        )).unwrap();
        for address in [addresses.pool, addresses.asset, addresses.collateral, addresses.oracle] {
            let expected = &state[address.to_string().to_ascii_lowercase()]["code"];
            assert!(expected.as_str().unwrap().len() > 2);
            let actual = local.rpc("eth_getCode", json!([address, "0x0"])).await;
            assert_eq!(&actual, expected, "seed code differs at {address}");
        }
        let latest = local.rpc("eth_getBlockByNumber", json!(["latest", false])).await;
        local.next_timestamp = quantity(&latest["timestamp"]) + 1;
        local
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        assert!(matches!(method, "eth_chainId" | "eth_blockNumber" | "eth_getBlockByNumber"
            | "eth_getCode" | "eth_getTransactionReceipt" | "anvil_impersonateAccount"
            | "evm_setNextBlockTimestamp" | "eth_sendTransaction" | "evm_mine"
            | "evm_snapshot" | "evm_revert"));
        let response: Value = self.client.post(self.url.clone())
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
        assert!(response.get("error").is_none(), "local {method}: {response}");
        response.get("result").expect("JSON-RPC result").clone()
    }

    async fn send<C: SolCall>(&mut self, from: Address, to: Address, call: C) -> Value {
        self.rpc("anvil_impersonateAccount", json!([from])).await;
        self.rpc("evm_setNextBlockTimestamp", json!([self.next_timestamp])).await;
        self.next_timestamp += 1;
        let transaction = self.rpc("eth_sendTransaction", json!([{
            "from":from, "to":to, "data":Bytes::from(call.abi_encode()),
            "gas":"0x989680", "gasPrice":"0x77359400"
        }])).await;
        for _ in 0..40 {
            let receipt = self.rpc("eth_getTransactionReceipt", json!([transaction])).await;
            if !receipt.is_null() {
                assert_eq!(receipt["status"], "0x1", "real protocol transaction reverted: {receipt}");
                return receipt;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("local transaction receipt did not arrive");
    }

    async fn mine_at(&mut self, timestamp: u64) {
        assert!(timestamp >= self.next_timestamp);
        self.rpc("evm_setNextBlockTimestamp", json!([timestamp])).await;
        self.rpc("evm_mine", json!([])).await;
        self.next_timestamp = timestamp + 1;
    }
}

fn quantity(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().strip_prefix("0x").unwrap(), 16).unwrap()
}

async fn deposit_asset(local: &mut LocalTransactions, a: &Addresses, user: Address, amount: u128) {
    local.send(user, a.asset, FixtureToken::mintCall { amount: U256::from(amount) }).await;
    local.send(user, a.asset, FixtureToken::approveCall { spender: a.pool, amount: U256::from(amount) }).await;
    local.send(user, a.pool, CreditPool::depositCall { recipient: user, amount }).await;
}

async fn deposit_collateral(local: &mut LocalTransactions, a: &Addresses, user: Address, market: u64, amount: u128) {
    local.send(user, a.collateral, FixtureToken::mintCall { amount: U256::from(amount) }).await;
    local.send(user, a.collateral, FixtureToken::approveCall { spender: a.pool, amount: U256::from(amount) }).await;
    local.send(user, a.pool, CreditPool::depositCollateralCall { account: user, market, amount }).await;
}

async fn assert_matches_getters(reader: &HttpReader, snapshot: &StateSnapshot) {
    for record in snapshot.positions.values() {
        let actual = reader.read_positions(record.key.pool, &[record.key], &snapshot.as_of).await.unwrap();
        assert_eq!(actual[&record.key], record.amounts);
        let hf = reader.contract_call(record.key.pool, &CreditPool::getHealthFactorCall {
            market: record.key.market, account: record.key.borrower,
        }, &snapshot.as_of).await.unwrap();
        let eligible = reader.contract_call(record.key.pool, &CreditPool::isLiquidatableCall {
            market: record.key.market, account: record.key.borrower,
        }, &snapshot.as_of).await.unwrap();
        let RiskStatus::Ready(risk) = &record.risk else { panic!("expected readable risk"); };
        assert_eq!(risk.health_factor, hf);
        assert_eq!(risk.eligible, eligible);
    }
}

fn amounts(snapshot: &StateSnapshot) -> BTreeMap<PositionKey, PositionAmounts> {
    snapshot.positions.iter().map(|(key, record)| (*key, record.amounts)).collect()
}

fn receipt_events<E: SolEvent>(receipt: &Value, pool: Address) -> Vec<E> {
    receipt["logs"].as_array().unwrap().iter()
        .filter(|log| log["address"].as_str().unwrap().eq_ignore_ascii_case(&pool.to_string())
            && log["topics"][0] == json!(E::SIGNATURE_HASH))
        .map(|log| {
            let topics: Vec<B256> = serde_json::from_value(log["topics"].clone()).unwrap();
            let data: Bytes = serde_json::from_value(log["data"].clone()).unwrap();
            E::decode_raw_log_validate(topics, &data).unwrap()
        }).collect()
}

async fn assert_cold_matches(reader: &Arc<HttpReader>, engine: &StateEngine<HttpReader>,
    spec: PoolSpec, config: &EngineConfig)
{
    let snapshot = engine.snapshot().unwrap();
    assert_matches_getters(reader, snapshot).await;
    let mut cold = StateEngine::new(reader.clone(), spec, config.clone()).unwrap();
    let rebuilt = cold.bootstrap_at(snapshot.as_of).await.unwrap();
    assert_eq!(amounts(&rebuilt), amounts(snapshot));
    assert_eq!(rebuilt.globals, snapshot.globals);
    assert_eq!(rebuilt.markets, snapshot.markets);
    assert_eq!(rebuilt.oracle, snapshot.oracle);
    // Independently enumerate current contract storage and compare complete debt
    // coverage with the historical engine at the same real EVM block.
    let mut current = barterlend_state::CurrentStateEngine::new(reader.clone(), spec, config.clone(),
        barterlend_state::CurrentStateConfig::default()).unwrap();
    let mut completed = false;
    for _ in 0..100 {
        if matches!(current.sync_step().await.unwrap().outcome, barterlend_state::SyncStepOutcome::Complete(_)) {
            completed = true; break;
        }
    }
    assert!(completed);
    let current = current.snapshot_shared().unwrap();
    assert_eq!(current.coverage, barterlend_state::CurrentCoverage::AllDebtPositionsV5);
    assert_eq!(current.state.as_of, snapshot.as_of);
    let debts = |state: &StateSnapshot| state.positions.iter().filter(|(_, p)| p.amounts.debt > 0)
        .map(|(key, p)| (*key, p.amounts)).collect::<BTreeMap<_, _>>();
    assert_eq!(debts(&current.state), debts(snapshot));
    assert_eq!(current.state.globals, snapshot.globals);
    assert_eq!(current.state.oracle, snapshot.oracle);
    for (key, record) in &snapshot.positions {
        assert_eq!(rebuilt.positions[key].risk, record.risk);
    }
}

async fn open_direct_debt(local: &mut LocalTransactions, a: &Addresses, market: u64) {
    deposit_collateral(local, a, a.borrower_a, market, 400 * WAD).await;
    deposit_asset(local, a, a.provider_a, 100 * WAD).await;
    local.send(a.provider_a, a.pool, CreditPool::placeProvideOrderCall {
        account: a.provider_a, market, APR: U256::ZERO, amount: 100 * WAD,
    }).await;
    let receipt = local.send(a.borrower_a, a.pool, CreditPool::borrowCall {
        account: a.borrower_a, market, amount: 100 * WAD, maxDebt: 100 * WAD,
    }).await;
    let events = receipt_events::<CreditPool::Borrowed>(&receipt, a.pool);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].endAPR, U256::ZERO);
    assert!(!receipt_events::<CreditPool::UnderlyingPoolIndexUpdated>(&receipt, a.pool).is_empty());
}

// The registry is append-only on chain, but closed entries must not consume
// the SDK's live-position limit or keep empty arbitrary markets in snapshots.
async fn closed_registry_and_market_reuse(local: &mut LocalTransactions, a: &Addresses,
    reader: &Arc<HttpReader>, spec: PoolSpec, config: &EngineConfig, market: u64)
{
    use barterlend_state::{CurrentStateEngine, CurrentStateConfig, SyncStepOutcome, StateUpdate};
    let saved = local.rpc("evm_snapshot", json!([])).await;
    let saved_timestamp = local.next_timestamp;
    for user in [a.borrower_a, a.borrower_b, a.provider_a] {
        deposit_collateral(local, a, user, market, 1).await;
        local.send(user, a.pool, CreditPool::withdrawCollateralCall {
            account: user, market, to: user, amount: 1,
        }).await;
    }
    let mut limited = config.clone(); limited.max_positions = 1;
    limited.reconcile_user_page_size = 1; limited.read_batch_size = 1;
    let mut engine = CurrentStateEngine::new(reader.clone(), spec, limited,
        CurrentStateConfig { max_calendar_markets: 1, ..Default::default() }).unwrap();
    let mut complete = false;
    for _ in 0..200 {
        if matches!(engine.sync_step().await.unwrap().outcome, SyncStepOutcome::Complete(_)) {
            complete = true; break;
        }
    }
    assert!(complete, "closed registry must not exhaust max_positions=1");
    let snapshot = engine.snapshot_shared().unwrap();
    assert!(snapshot.state.positions.is_empty());
    assert!(reader.market_users(a.pool, market, 0, 10, &snapshot.state.as_of).await.unwrap().len() >= 3);
    let arbitrary = market + 12345;
    let key = PositionKey { pool: a.pool, market: arbitrary, borrower: a.borrower_a };
    for withdraw in [false, true, false] {
        if withdraw {
            local.send(a.borrower_a, a.pool, CreditPool::withdrawCollateralCall {
                account: a.borrower_a, market: arbitrary, to: a.borrower_a, amount: 1,
            }).await;
        } else { deposit_collateral(local, a, a.borrower_a, arbitrary, 1).await; }
        let mut outcome = None;
        for _ in 0..200 {
            if let SyncStepOutcome::Complete(result) = engine.sync_step().await.unwrap().outcome {
                outcome = Some(result); break;
            }
        }
        let result = outcome.expect("market reuse must finish");
        let snapshot = engine.snapshot_shared().unwrap();
        assert_eq!(snapshot.state.positions.contains_key(&key), !withdraw);
        assert_eq!(snapshot.state.markets.contains_key(&arbitrary), !withdraw);
        if withdraw {
            let SyncOutcome::Advanced(batch) = result else { panic!("incremental update expected"); };
            assert!(batch.changes.contains(&StateUpdate::MarketRemoved(arbitrary)));
        }
        assert_matches_getters(reader, &snapshot.state).await;
        engine.checkpoint_builder().unwrap().build().unwrap().validate(None, None).unwrap();
    }
    assert_eq!(local.rpc("evm_revert", json!([saved])).await, json!(true));
    local.next_timestamp = saved_timestamp;
}

#[tokio::test]
#[ignore = "requires isolated seeded Anvil; run tests/runners/test-local-protocol.sh"]
async fn real_credit_pool_bootstrap_events_checkpoint_gap_and_oracle_clock() {
    let endpoint = std::env::var("BARTERLEND_LOCAL_RPC").expect("use tests/runners/test-local-protocol.sh");
    let a: Addresses = serde_json::from_str(include_str!(
        "fixtures/protocol-v5/generated/addresses.json"
    )).unwrap();
    let market: u64 = a.market0.parse().unwrap();
    let mut local = LocalTransactions::checked(&endpoint, &a).await;
    let reader = Arc::new(HttpReader::new(&endpoint, RpcConfig {
        pinning: BlockPinning::NumberBracket, min_request_interval: Duration::ZERO,
        max_retries: 0, ..RpcConfig::default()
    }).unwrap());
    let identity = reader.identity().await.unwrap();
    assert_eq!(identity.chain_id, 31337);
    assert_eq!(identity.genesis_hash, GENESIS);
    let spec = PoolSpec { address: a.pool, deployment_block: 0,
        deployment_block_hash: Some(GENESIS), expected_version: 5 };
    let config = EngineConfig { commitment: Commitment::Latest,
        expected_identity: Some(identity), operation_timeout: Duration::from_secs(30),
        log_chunk_size: 4, reconcile_positions_per_sync: 0,
        reconcile_user_pages_per_sync: 0, ..EngineConfig::default() };
    closed_registry_and_market_reuse(&mut local, &a, &reader, spec, &config, market).await;
    let mut engine = StateEngine::new(reader.clone(), spec, config.clone()).unwrap();
    let initial = engine.bootstrap().await.unwrap();
    assert_eq!(initial.as_of.number, 0);
    assert!(initial.positions.is_empty());
    // Native view bundles must match independent eth_calls on a real pool,
    // including dynamic arrays and a borrower-aware quote, without mining.
    let views=[
        barterlend_state::PinnedRead::call(a.pool,&CreditPool::assetCall{}),
        barterlend_state::PinnedRead::call(a.pool,&CreditPool::getActiveMarketsCall{}),
        barterlend_state::PinnedRead::call(a.pool,&CreditPool::getProvideOrderbookCall{market}),
        barterlend_state::PinnedRead::call(a.pool,&CreditPool::previewBorrowCall{market,amount:2000,borrower:a.borrower_a}),
        barterlend_state::PinnedRead::call(a.pool,&CreditPool::debtOfCall{market,account:a.borrower_a})];
    let independent=reader.read_bundle(&views,&initial.as_of).await.unwrap();
    assert_eq!(reader.read_pool_views(a.pool,&views,&initial.as_of).await.unwrap(),independent);
    assert_eq!(reader.head(Commitment::Latest).await.unwrap(),initial.as_of);
    let batched=(*reader).clone().with_read_optimizations(barterlend_state::ReadOptimizationConfig {
        position_batch_size:128,coalesce_pending_checks:true,..Default::default()}).unwrap();
    let batch_keys:Vec<_>=(1..=200).map(|n|PositionKey {pool:a.pool,market:market+n,
        borrower:a.borrower_a}).collect();
    assert_eq!(batched.read_positions(a.pool,&batch_keys,&initial.as_of).await.unwrap(),
        reader.read_positions(a.pool,&batch_keys,&initial.as_of).await.unwrap());



    deposit_collateral(&mut local, &a, a.borrower_a, market, 400 * WAD).await;
    local.send(a.borrower_a, a.pool, CreditPool::placeBorrowOrderCall {
        account: a.borrower_a, market, APR: U256::from(1000), amount: 100 * WAD,
    }).await;
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Advanced(_)));
    let key = PositionKey { pool: a.pool, market, borrower: a.borrower_a };
    assert_eq!(engine.snapshot().unwrap().positions[&key].amounts,
        PositionAmounts { debt: 0, collateral: 400 * WAD });
    deposit_asset(&mut local, &a, a.provider_a, 100 * WAD).await;
    let receipt = local.send(a.provider_a, a.pool, CreditPool::provideExactInCall {
        account: a.provider_a, market, assetsAmount: 100 * WAD,
        minLoanAmount: 0, maxCrossedOrders: 10,
    }).await;
    let pool_logs: Vec<_> = receipt["logs"].as_array().unwrap().iter()
        .filter(|log| log["address"].as_str().unwrap().eq_ignore_ascii_case(&a.pool.to_string())).collect();
    assert!(pool_logs.iter().any(|log| log["topics"][0] == json!(CreditPool::Provided::SIGNATURE_HASH)));
    assert!(!pool_logs.iter().any(|log| log["topics"][0] == json!(CreditPool::Borrowed::SIGNATURE_HASH)));
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Advanced(_)));
    let borrowed = engine.snapshot().unwrap();
    assert_eq!(borrowed.positions[&key].amounts.collateral, 400 * WAD);
    assert!(borrowed.positions[&key].amounts.debt > 100 * WAD);
    assert_matches_getters(&reader, borrowed).await;
    let checkpoint: Checkpoint = serde_json::from_slice(
        &serde_json::to_vec(&engine.checkpoint().unwrap()).unwrap()
    ).unwrap();
    let debt_before = borrowed.positions[&key].amounts.debt;

    // Intentionally skip intermediate syncs and mutate an arbitrary, inactive market.
    deposit_collateral(&mut local, &a, a.borrower_b, 0, 25 * WAD).await;
    deposit_asset(&mut local, &a, a.provider_b, 55 * WAD).await;
    local.send(a.provider_b, a.pool, CreditPool::repayCall {
        account: a.provider_b, borrower: a.borrower_a, market, amount: 55 * WAD,
    }).await;
    for _ in 0..3 { local.mine_at(local.next_timestamp).await; }
    assert!(matches!(engine.sync().await.unwrap(), SyncOutcome::Advanced(_)));
    let synced = engine.snapshot().unwrap().clone();
    assert_eq!(synced.positions[&key].amounts.debt, debt_before - 55 * WAD);
    let collateral_key = PositionKey { pool: a.pool, market: 0, borrower: a.borrower_b };
    assert_eq!(synced.positions[&collateral_key].amounts, PositionAmounts { debt: 0, collateral: 25 * WAD });
    assert!(!synced.positions.keys().any(|key| key.borrower == a.provider_b));
    assert_matches_getters(&reader, &synced).await;
    let mut restored = StateEngine::restore(reader.clone(), spec, config.clone(), checkpoint).unwrap();
    assert!(matches!(restored.sync().await.unwrap(), SyncOutcome::Advanced(_)));
    assert_eq!(restored.snapshot().unwrap(), &synced);
    assert!(matches!(restored.sync().await.unwrap(), SyncOutcome::Unchanged { .. }));

    // No pool event accompanies a mock oracle update; all open risks must refresh.
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall {
        newPrice: U256::from(WAD / 10),
    }).await;
    restored.sync().await.unwrap();
    assert_matches_getters(&reader, restored.snapshot().unwrap()).await;
    let RiskStatus::Ready(risk) = &restored.snapshot().unwrap().positions[&key].risk else { panic!("fresh risk"); };
    assert!(risk.eligible);
    let raw_before_stale = amounts(restored.snapshot().unwrap());
    let last = restored.snapshot().unwrap().as_of.timestamp;
    local.mine_at(last + restored.snapshot().unwrap().globals.stale_window).await;
    restored.sync().await.unwrap();
    let stale = restored.snapshot().unwrap();
    assert_eq!(amounts(stale), raw_before_stale);
    assert!(matches!(stale.oracle, OracleStatus::Unavailable(OracleFailure::Stale { .. })));
    assert!(matches!(stale.positions[&key].risk,
        RiskStatus::Unavailable(RiskFailure::Oracle(OracleFailure::Stale { .. }))));
    let mut rebuilt = StateEngine::new(reader.clone(), spec, config.clone()).unwrap();
    let cold = rebuilt.bootstrap_at(stale.as_of).await.unwrap();
    assert_eq!(amounts(&cold), amounts(stale));
    assert_eq!(cold.oracle, stale.oracle);

    // Real collateral move/withdraw must invalidate both keys, including inactive markets.
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall { newPrice: U256::from(WAD) }).await;
    let far_market = u64::MAX - 100;
    local.send(a.borrower_b, a.pool, CreditPool::moveCollateralCall {
        account: a.borrower_b, fromMarket: 0, toMarket: far_market, amount: 25 * WAD,
    }).await;
    restored.sync().await.unwrap();
    let far_key = PositionKey { market: far_market, ..collateral_key };
    assert!(!restored.snapshot().unwrap().positions.contains_key(&collateral_key));
    assert_eq!(restored.snapshot().unwrap().positions[&far_key].amounts,
        PositionAmounts { debt: 0, collateral: 25 * WAD });
    assert_cold_matches(&reader, &restored, spec, &config).await;
    local.send(a.borrower_b, a.pool, CreditPool::withdrawCollateralCall {
        account: a.borrower_b, market: far_market, to: a.provider_b, amount: 25 * WAD,
    }).await;
    restored.sync().await.unwrap();
    assert!(!restored.snapshot().unwrap().positions.contains_key(&far_key));

    // Full repayment preserves collateral; only the final withdrawal removes a position.
    let remaining = restored.snapshot().unwrap().positions[&key].amounts.debt;
    deposit_asset(&mut local, &a, a.provider_b, remaining).await;
    local.send(a.provider_b, a.pool, CreditPool::repayCall {
        account: a.provider_b, borrower: a.borrower_a, market, amount: remaining,
    }).await;
    restored.sync().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().positions[&key].amounts,
        PositionAmounts { debt: 0, collateral: 400 * WAD });
    assert_cold_matches(&reader, &restored, spec, &config).await;
    local.send(a.borrower_a, a.pool, CreditPool::withdrawCollateralCall {
        account: a.borrower_a, market, to: a.borrower_a, amount: 400 * WAD,
    }).await;
    restored.sync().await.unwrap();
    assert!(restored.snapshot().unwrap().positions.is_empty());
    assert_cold_matches(&reader, &restored, spec, &config).await;
    println!("PASS: real move/withdraw and full repayment retain/remove exactly the expected position keys");

    // Borrowed discovery, partial liquidation and collateral-draining bad debt use real pool logs.
    open_direct_debt(&mut local, &a, market).await;
    restored.sync().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().positions[&key].amounts.debt, 100 * WAD);
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall {
        newPrice: U256::from(WAD * 3 / 10),
    }).await;
    local.send(a.provider_b, a.asset, FixtureToken::mintCall { amount: U256::from(100 * WAD) }).await;
    local.send(a.provider_b, a.asset, FixtureToken::approveCall {
        spender: a.pool, amount: U256::from(100 * WAD),
    }).await;
    local.send(a.provider_b, a.pool, CreditPool::liquidateCall {
        market, borrower: a.borrower_a, debtAmount: 10 * WAD,
        minDiscountOut: U256::ZERO, deadline: U256::from(local.next_timestamp), data: Bytes::new(),
    }).await;
    restored.sync().await.unwrap();
    let partial = restored.snapshot().unwrap().positions[&key].amounts;
    assert_eq!(partial.debt, 90 * WAD);
    assert!(partial.collateral > 0 && partial.collateral < 400 * WAD);
    assert_cold_matches(&reader, &restored, spec, &config).await;
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall {
        newPrice: U256::from(WAD / 10),
    }).await;
    let receipt = local.send(a.provider_b, a.pool, CreditPool::liquidateCall {
        market, borrower: a.borrower_a, debtAmount: partial.debt,
        minDiscountOut: U256::ZERO, deadline: U256::from(local.next_timestamp), data: Bytes::new(),
    }).await;
    assert!(receipt["logs"].as_array().unwrap().iter().any(|log|
        log["address"].as_str().unwrap().eq_ignore_ascii_case(&a.pool.to_string())
            && log["topics"][0] == json!(CreditPool::BadDebtIncurred::SIGNATURE_HASH)));
    restored.sync().await.unwrap();
    assert!(!restored.snapshot().unwrap().positions.contains_key(&key));
    assert_cold_matches(&reader, &restored, spec, &config).await;
    println!("PASS: real direct borrow, partial liquidation and bad-debt drain converge without reconciliation");

    // Loan transfers alone are not borrower debt; offset changes debt but leaves collateral.
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall { newPrice: U256::from(WAD) }).await;
    open_direct_debt(&mut local, &a, market).await;
    local.send(a.provider_a, a.pool, CreditPool::transferLoanCall {
        market, to: a.borrower_a, amount: 100 * WAD,
    }).await;
    restored.sync().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().positions.len(), 1);
    assert_eq!(restored.snapshot().unwrap().positions[&key].amounts.debt, 100 * WAD);
    local.send(a.borrower_a, a.pool, CreditPool::offsetCall {
        account: a.borrower_a, market, amount: 100 * WAD,
    }).await;
    restored.sync().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().positions[&key].amounts,
        PositionAmounts { debt: 0, collateral: 400 * WAD });
    assert_cold_matches(&reader, &restored, spec, &config).await;
    local.send(a.borrower_a, a.pool, CreditPool::withdrawCollateralCall {
        account: a.borrower_a, market, to: a.borrower_a, amount: 400 * WAD,
    }).await;
    restored.sync().await.unwrap();
    assert!(restored.snapshot().unwrap().positions.is_empty());
    println!("PASS: actual loan transfer/offset preserve the debt-versus-loan distinction");

    // Maturity equality changes risk without pool events; refinance moves source and target keys.
    open_direct_debt(&mut local, &a, market).await;
    let next_market: u64 = a.market1.parse().unwrap();
    deposit_asset(&mut local, &a, a.provider_b, 200 * WAD).await;
    local.send(a.provider_b, a.pool, CreditPool::placeProvideOrderCall {
        account: a.provider_b, market: next_market, APR: U256::ZERO, amount: 200 * WAD,
    }).await;
    restored.sync().await.unwrap();
    let grace = restored.snapshot().unwrap().markets[&market].grace_period;
    local.mine_at(market + grace - 2).await;
    local.send(a.borrower_a, a.oracle, FixtureOracle::setPriceCall { newPrice: U256::from(WAD) }).await;
    restored.sync().await.unwrap();
    let RiskStatus::Ready(before) = &restored.snapshot().unwrap().positions[&key].risk else { panic!("fresh maturity risk"); };
    assert!(!before.eligible);
    local.mine_at(market + grace).await;
    restored.sync().await.unwrap();
    let RiskStatus::Ready(at) = &restored.snapshot().unwrap().positions[&key].risk else { panic!("fresh maturity risk"); };
    assert!(at.eligible && at.health_factor >= U256::from(WAD));
    assert_cold_matches(&reader, &restored, spec, &config).await;
    local.send(a.borrower_a, a.pool, CreditPool::refinanceDebtsCall {
        account: a.borrower_a, fromMarket: market, maxDebtFactor: U256::from(WAD / 5),
        refinances: vec![barterlend_state::abi::ICreditPoolTypes::RefinanceDebtParams {
            toMarket: next_market, debtToRepay: 100 * WAD, assetsToBorrow: 100 * WAD,
        }],
    }).await;
    restored.sync().await.unwrap();
    let next_key = PositionKey { market: next_market, ..key };
    assert!(!restored.snapshot().unwrap().positions.contains_key(&key));
    assert_eq!(restored.snapshot().unwrap().positions[&next_key].amounts,
        PositionAmounts { debt: 100 * WAD, collateral: 400 * WAD });
    assert_cold_matches(&reader, &restored, spec, &config).await;
    let final_checkpoint = restored.checkpoint().unwrap();
    let mut restarted = StateEngine::restore(reader.clone(), spec, config, final_checkpoint).unwrap();
    assert!(matches!(restarted.sync().await.unwrap(), SyncOutcome::Unchanged { .. }));
    assert_eq!(restarted.snapshot().unwrap(), restored.snapshot().unwrap());
    println!("PASS: real grace boundary/refinance and final checkpoint restore agree with canonical getters");

    // Replace a published branch on the actual RPC node, including event/header metadata.
    let fork = local.rpc("evm_snapshot", json!([])).await;
    deposit_collateral(&mut local, &a, a.borrower_b, 0, 3 * WAD).await;
    restarted.sync().await.unwrap();
    assert_eq!(restarted.snapshot().unwrap().positions[&collateral_key].amounts.collateral, 3 * WAD);
    let replaced_epoch = restarted.snapshot().unwrap().epoch;
    let replaced_block = restarted.snapshot().unwrap().as_of;
    assert_eq!(local.rpc("evm_revert", json!([fork])).await, json!(true));
    let ancestor = local.rpc("eth_getBlockByNumber", json!(["latest", false])).await;
    local.next_timestamp = quantity(&ancestor["timestamp"]) + 1;
    deposit_collateral(&mut local, &a, a.borrower_b, far_market, 7 * WAD).await;
    let SyncOutcome::Reset(reset) = restarted.sync().await.unwrap() else { panic!("real branch replacement must reset"); };
    assert_eq!(reset.as_of.number, replaced_block.number);
    assert_ne!(reset.as_of.hash, replaced_block.hash);
    assert_eq!(reset.epoch, replaced_epoch + 1);
    assert!(!reset.positions.contains_key(&collateral_key));
    assert_eq!(reset.positions[&far_key].amounts.collateral, 7 * WAD);
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    println!("PASS: actual Anvil branch replacement emits Reset, increments epoch and removes orphaned keys");

    // Direct reloan and a Provided reloan fill only transfer lender exposure, never borrower debt.
    let before_reloan = amounts(restarted.snapshot().unwrap());
    deposit_asset(&mut local, &a, a.provider_a, 50 * WAD).await;
    local.send(a.provider_a, a.pool, CreditPool::placeProvideOrderCall {
        account: a.provider_a, market: next_market, APR: U256::ZERO, amount: 50 * WAD,
    }).await;
    let receipt = local.send(a.provider_b, a.pool, CreditPool::reloanCall {
        account: a.provider_b, market: next_market, amount: 50 * WAD, minAssets: 50 * WAD,
    }).await;
    assert_eq!(receipt_events::<CreditPool::Reloaned>(&receipt, a.pool).len(), 1);
    restarted.sync().await.unwrap();
    assert_eq!(amounts(restarted.snapshot().unwrap()), before_reloan);
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    local.send(a.provider_b, a.pool, CreditPool::placeReloanOrderCall {
        account: a.provider_b, market: next_market, APR: U256::ZERO, amount: 25 * WAD,
    }).await;
    deposit_asset(&mut local, &a, a.provider_c, 25 * WAD).await;
    let receipt = local.send(a.provider_c, a.pool, CreditPool::provideExactInCall {
        account: a.provider_c, market: next_market, assetsAmount: 25 * WAD,
        minLoanAmount: 25 * WAD, maxCrossedOrders: 10,
    }).await;
    let provided = receipt_events::<CreditPool::Provided>(&receipt, a.pool);
    assert_eq!(provided.len(), 1);
    assert_eq!(provided[0].fills.len(), 1);
    assert!(provided[0].fills[0].isReloan);
    assert_eq!(provided[0].fills[0].account, a.provider_b);
    restarted.sync().await.unwrap();
    assert_eq!(amounts(restarted.snapshot().unwrap()), before_reloan);
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    println!("PASS: actual direct reloan and Provided reloan fill leave borrower state unchanged");

    // Sweep emits Borrowed for its reward beneficiary but creates debt only for Provided borrowers.
    deposit_collateral(&mut local, &a, a.borrower_b, next_market, 400 * WAD).await;
    local.send(a.borrower_b, a.pool, CreditPool::placeBorrowOrderCall {
        account: a.borrower_b, market: next_market, APR: U256::from(2000), amount: 100 * WAD,
    }).await;
    restarted.sync().await.unwrap();
    let swept_key = PositionKey { pool: a.pool, market: next_market, borrower: a.borrower_b };
    assert_eq!(restarted.snapshot().unwrap().positions[&swept_key].amounts.debt, 0);
    let receipt = local.send(a.provider_c, a.pool, CreditPool::sweepCall {
        account: a.provider_c, market: next_market, amount: 50 * WAD, maxCrossedOrders: 10,
    }).await;
    let borrowed = receipt_events::<CreditPool::Borrowed>(&receipt, a.pool);
    assert_eq!(borrowed.len(), 1);
    assert_eq!(borrowed[0].account, a.provider_c);
    assert_eq!(receipt_events::<CreditPool::Swept>(&receipt, a.pool).len(), 1);
    assert!(receipt_events::<CreditPool::Provided>(&receipt, a.pool).iter()
        .flat_map(|event| event.fills.iter())
        .any(|fill| !fill.isReloan && fill.account == a.borrower_b));
    restarted.sync().await.unwrap();
    assert_eq!(restarted.snapshot().unwrap().positions[&swept_key].amounts.debt, borrowed[0].debtIncurred);
    assert_eq!(restarted.snapshot().unwrap().positions[&swept_key].amounts.collateral, 400 * WAD);
    assert!(!restarted.snapshot().unwrap().positions.keys().any(|key| key.borrower == a.provider_c));
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    println!("PASS: actual sweep discovers borrower debt without creating reward-beneficiary debt");

    // Admin changes update global min debt, while cached markets keep their original grace/discount.
    let cached_config = restarted.snapshot().unwrap().markets[&next_market];
    let before_admin = amounts(restarted.snapshot().unwrap());
    let receipt = local.send(a.admin, a.pool, CreditPool::setLiquidationDiscountMinCall {
        newMin: U256::from(WAD * 4 / 100),
    }).await;
    assert_eq!(receipt_events::<CreditPool::AdminConfigUpdated>(&receipt, a.pool).len(), 1);
    local.send(a.admin, a.pool, CreditPool::setLiquidationGracePeriodCall {
        newGracePeriod: 2 * 24 * 60 * 60,
    }).await;
    local.send(a.admin, a.pool, CreditPool::setMinDebtRemainingAfterLiquidationCall {
        newMin: 15 * WAD,
    }).await;
    restarted.sync().await.unwrap();
    let configured = restarted.snapshot().unwrap();
    assert_eq!(configured.globals.min_debt_remaining, 15 * WAD);
    assert_eq!(configured.markets[&next_market], cached_config);
    assert_eq!(configured.markets[&far_market].grace_period, 2 * 24 * 60 * 60);
    assert_eq!(configured.markets[&far_market].discount_min, U256::from(WAD * 4 / 100));
    assert_eq!(amounts(configured), before_admin);
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    println!("PASS: actual admin updates preserve cached market configs and refresh arbitrary-market defaults");

    // Claim is outside borrower accounting: exercise its real event and preserve every borrower key.
    let before_claim = amounts(restarted.snapshot().unwrap());
    let claim_loan = CreditPool::loanOfCall { market, account: a.provider_a };
    let loan_before = reader.contract_call(a.pool, &claim_loan, &restarted.snapshot().unwrap().as_of).await.unwrap();
    assert!(loan_before >= WAD);
    let receipt = local.send(a.provider_a, a.pool, CreditPool::claimCall {
        account: a.provider_a, market, amount: WAD,
    }).await;
    assert_eq!(receipt_events::<CreditPool::Claimed>(&receipt, a.pool).len(), 1);
    restarted.sync().await.unwrap();
    let loan_after = reader.contract_call(a.pool, &claim_loan, &restarted.snapshot().unwrap().as_of).await.unwrap();
    assert!(loan_after < loan_before);
    assert_eq!(amounts(restarted.snapshot().unwrap()), before_claim);
    assert_cold_matches(&reader, &restarted, spec, &restarted.config().clone()).await;
    println!("PASS: actual matured lender claim leaves borrower state unchanged");
}
