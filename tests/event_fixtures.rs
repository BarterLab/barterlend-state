//! Expected keys are specified independently of decoded event fields.
//! Logs come from CreditPool v5; block and transaction metadata below are synthetic
//! because Foundry vm.recordLogs captures emitter/topics/data, not RPC receipts.

use std::collections::BTreeSet;

use alloy_primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy_sol_types::SolEvent;
use barterlend_state::{Error, PositionKey, RawLog, abi::CreditPool, events::{Invalidations, decode_invalidation, normalize_logs}};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixtureLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    pool: Address,
    block_number: String,
    logs: Vec<FixtureLog>,
}

#[derive(Debug, Deserialize)]
struct Addresses {
    pool: Address,
    borrower_a: Address,
    borrower_b: Address,
    provider_a: Address,
    provider_b: Address,
    provider_c: Address,
    market0: String,
    market1: String,
}

fn addresses() -> Addresses {
    serde_json::from_str(include_str!("fixtures/protocol-v5/generated/addresses.json")).unwrap()
}

fn fixture(name: &str) -> Fixture {
    let json = match name {
        "borrowed" => include_str!("fixtures/protocol-v5/generated/borrowed.json"),
        "reloaned" => include_str!("fixtures/protocol-v5/generated/reloaned.json"),
        "underlying-index" => include_str!("fixtures/protocol-v5/generated/underlying-index.json"),
        "provided-borrow" => include_str!("fixtures/protocol-v5/generated/provided-borrow.json"),
        "provided-mixed" => include_str!("fixtures/protocol-v5/generated/provided-mixed.json"),
        "provided-all-arrays" => include_str!("fixtures/protocol-v5/generated/provided-all-arrays.json"),
        "sweep" => include_str!("fixtures/protocol-v5/generated/sweep.json"),
        "refinanced" => include_str!("fixtures/protocol-v5/generated/refinanced.json"),
        "repaid-other-payer" => include_str!("fixtures/protocol-v5/generated/repaid-other-payer.json"),
        "collateral" => include_str!("fixtures/protocol-v5/generated/collateral.json"),
        "offset" => include_str!("fixtures/protocol-v5/generated/offset.json"),
        "liquidated-bad-debt" => include_str!("fixtures/protocol-v5/generated/liquidated-bad-debt.json"),
        "admin" => include_str!("fixtures/protocol-v5/generated/admin.json"),
        _ => panic!("unknown fixture {name}"),
    };
    let result: Fixture = serde_json::from_str(json).unwrap();
    assert_eq!(result.name, name);
    assert_eq!(result.pool, addresses().pool);
    result
}

fn raw_logs(fixture: Fixture) -> Vec<RawLog> {
    let number = fixture.block_number.parse::<u64>().unwrap();
    fixture.logs.into_iter().enumerate().map(|(index, log)| RawLog {
        address: log.address,
        topics: log.topics,
        data: log.data,
        block_number: number,
        block_hash: B256::repeat_byte(0x11),
        transaction_hash: B256::repeat_byte(0x22),
        log_index: index as u64,
        removed: false,
    }).collect()
}

fn invalidations(name: &str) -> Invalidations {
    let pool = addresses().pool;
    let mut result = Invalidations::default();
    for log in raw_logs(fixture(name)) {
        result.extend(decode_invalidation(pool, &log).unwrap());
    }
    result
}

fn key(market: u64, borrower: Address) -> PositionKey {
    PositionKey { pool: addresses().pool, market, borrower }
}

fn borrower_market0() -> PositionKey {
    let config = addresses();
    key(config.market0.parse().unwrap(), config.borrower_a)
}

#[test]
fn real_direct_borrow_identifies_borrower() {
    let logs = raw_logs(fixture("borrowed"));
    assert_eq!(logs[0].topics[0], keccak256("Borrowed(address,uint64,uint128,uint128,uint256)"));
    assert_eq!(logs[0].data.len(), 96);
    let result = invalidations("borrowed");
    assert_eq!(result.positions, BTreeSet::from([borrower_market0()]));
    assert_eq!(result.markets, BTreeSet::from([31_536_001]));
    assert!(!result.config_changed);
}

#[test]
fn real_reloan_with_apr_and_underlying_index_do_not_create_borrower_positions() {
    let reloan = raw_logs(fixture("reloaned")).remove(0);
    assert_eq!(reloan.topics[0], keccak256("Reloaned(address,uint64,uint128,uint128,uint256,uint256)"));
    let value = CreditPool::Reloaned::decode_raw_log_validate(reloan.topics.iter().copied(), &reloan.data).unwrap();
    assert_eq!(value.endAPR, U256::ZERO);
    assert_eq!(invalidations("reloaned"), Invalidations::default());

    let logs = raw_logs(fixture("underlying-index"));
    let index = logs.iter().find(|log| log.topics[0] == keccak256("UnderlyingPoolIndexUpdated(uint256,uint64)")).unwrap();
    let value = CreditPool::UnderlyingPoolIndexUpdated::decode_raw_log_validate(index.topics.iter().copied(), &index.data).unwrap();
    assert_eq!(value.index, U256::from(1_100_000_000_000_000_000_000_000_000_u128));
    assert_eq!(value.generation, 0);
    assert_eq!(invalidations("underlying-index"), Invalidations::default());

    for log in [reloan, index.clone()] {
        let mut truncated = log.clone();
        truncated.data = truncated.data[..truncated.data.len() - 1].to_vec().into();
        assert!(matches!(decode_invalidation(addresses().pool, &truncated), Err(Error::Decode(_))));
        let mut trailing = log;
        let mut bytes = trailing.data.to_vec();
        bytes.extend_from_slice(&[0; 32]);
        trailing.data = bytes.into();
        assert!(matches!(decode_invalidation(addresses().pool, &trailing), Err(Error::Decode(_))));
    }
}

#[test]
fn legacy_borrow_event_is_rejected_instead_of_silently_advancing_the_cursor() {
    let mut log = raw_logs(fixture("borrowed")).remove(0);
    log.topics[0] = keccak256("Borrowed(address,uint64,uint128,uint128)");
    log.data = log.data[..64].to_vec().into();
    assert!(matches!(decode_invalidation(addresses().pool, &log), Err(Error::Decode(_))));
}

#[test]
fn real_provided_dynamic_fill_discovers_debt_without_borrowed_event() {
    let logs = raw_logs(fixture("provided-borrow"));
    assert_eq!(logs.len(), 1);
    // Literal canonical Solidity signature: does not reference the Rust binding.
    let provided = keccak256("Provided(address,uint64,uint128,uint128,(address,uint256,uint128,uint128,bool)[],(address,uint256,uint128)[],(address,uint256,uint128)[])");
    assert_eq!(logs[0].topics[0], provided);
    assert_ne!(logs[0].topics[0], keccak256("Borrowed(address,uint64,uint128,uint128,uint256)"));
    let result = invalidations("provided-borrow");
    assert_eq!(result.positions, BTreeSet::from([borrower_market0()]));
    assert!(!result.positions.contains(&key(31_536_001, addresses().provider_a)));
}

#[test]
fn reloan_flag_is_not_interpreted_as_new_debt() {
    let mut log = raw_logs(fixture("provided-borrow")).remove(0);
    // Canonical synthetic reloan mutation of a real one-fill event. This tests
    // decoder classification, not actual mixed matching execution by Solidity.
    let mut data = log.data.to_vec();
    assert_eq!(data[10 * 32 + 31], 0);
    data[10 * 32 + 31] = 1;
    log.data = data.into();
    let result = decode_invalidation(addresses().pool, &log).unwrap();
    assert!(result.positions.is_empty());
    assert_eq!(result.markets, BTreeSet::from([31_536_001]));
}

#[test]
fn real_mixed_provided_refreshes_borrower_without_reloan_lender() {
    let result = invalidations("provided-mixed");
    assert_eq!(result.positions, BTreeSet::from([key(31_536_001, addresses().borrower_b)]));
    assert_eq!(result.markets, BTreeSet::from([31_536_001]));
    assert!(!result.positions.contains(&key(31_536_001, addresses().provider_a)));
    assert!(!result.positions.contains(&borrower_market0()));
}

#[test]
fn real_provided_all_dynamic_arrays_do_not_create_phantom_borrower_debt() {
    let logs = raw_logs(fixture("provided-all-arrays"));
    assert_eq!(logs.len(), 1);
    let log = &logs[0];
    let pool = address!("2a07706473244bc757e10f2a9e86fb532828afe3");
    let borrower = address!("9f3dddcba34e4496824d9b1fa4373a33440575b1");
    let capped_borrower = address!("163693257f2919e04965fb83f8d83b0781c81929");
    let reloan_a = address!("7400e36c54e12494c662c40a3e6614a85881bc7a");
    let reloan_c = address!("7bd8bbb6c6b1c6e03091cdaaab00ce695e2427df");
    assert_eq!(log.address, pool);
    assert_eq!(log.topics[0], keccak256("Provided(address,uint64,uint128,uint128,(address,uint256,uint128,uint128,bool)[],(address,uint256,uint128)[],(address,uint256,uint128)[])"));

    // Validate all tuple arrays against the actual full v5 ABI, independently of
    // the invalidation decoder. The Solidity scenario verifies that liquidation
    // leaves 16e18 loan backing for each reloan, reducing the 50e18 orders by 34e18.
    let event = CreditPool::Provided::decode_raw_log_validate(log.topics.iter().copied(), &log.data).unwrap();
    assert_eq!(event.market, 31_536_001);
    assert_eq!(event.fills.len(), 4);
    assert_eq!(event.fills.iter().map(|fill| (fill.account, fill.isReloan, fill.assets)).collect::<Vec<_>>(), vec![
        (reloan_a, true, 16_000_000_000_000_000_000),
        (reloan_c, true, 16_000_000_000_000_000_000),
        (capped_borrower, false, 2_500_000_000_000_000_000),
        (borrower, false, 5_500_000_000_000_000_000),
    ]);
    assert_eq!(event.invalidatedOrders.len(), 1);
    assert_eq!(event.invalidatedOrders[0].account, capped_borrower);
    assert_eq!(event.invalidatedOrders[0].amount, 7_500_000_000_000_000_000);
    assert_eq!(event.reconciledOrders.len(), 2);
    assert_eq!(event.reconciledOrders.iter().map(|order| (order.account, order.amount)).collect::<Vec<_>>(), vec![
        (reloan_a, 34_000_000_000_000_000_000),
        (reloan_c, 34_000_000_000_000_000_000),
    ]);
    let encoded = event.encode_log_data();
    assert_eq!(encoded.topics(), log.topics);
    assert_eq!(encoded.data, log.data);

    let result = decode_invalidation(pool, log).unwrap();
    assert_eq!(result.positions, BTreeSet::from([
        PositionKey { pool, market: 31_536_001, borrower },
        PositionKey { pool, market: 31_536_001, borrower: capped_borrower },
    ]));
    assert_eq!(result.markets, BTreeSet::from([31_536_001]));
    assert!(!result.config_changed);
    for account in [reloan_a, reloan_c, addresses().provider_b] {
        assert!(!result.positions.contains(&PositionKey { pool, market: 31_536_001, borrower: account }));
    }
    assert_eq!(invalidations("provided-all-arrays"), result);
}

#[test]
fn real_sweep_marks_borrower_and_conservatively_refreshes_reward_beneficiary() {
    let result = invalidations("sweep");
    assert_eq!(result.positions, BTreeSet::from([
        borrower_market0(),
        key(31_536_001, addresses().provider_c),
    ]));
    let logs = raw_logs(fixture("sweep"));
    assert!(logs.iter().any(|log| log.topics[0] == keccak256("Borrowed(address,uint64,uint128,uint128,uint256)")));
    assert!(logs.iter().any(|log| log.topics[0] == keccak256("Swept(address,uint64,uint128,uint128)")));
    // Invalidations contain hints, not debt amounts; getters decide whether a key is stored.
    let swept = logs.iter().find(|log| log.topics[0] == keccak256("Swept(address,uint64,uint128,uint128)")).unwrap();
    assert!(decode_invalidation(addresses().pool, swept).unwrap().positions.is_empty());
}

#[test]
fn real_refinance_five_argument_event_refreshes_source_and_target() {
    let logs = raw_logs(fixture("refinanced"));
    let literal = keccak256("DebtRefinanced(address,uint64,uint64,uint128,uint128)");
    let log = logs.iter().find(|log| log.topics[0] == literal).unwrap();
    assert_eq!(log.data.len(), 64);
    assert_ne!(log.topics[0], keccak256("DebtRefinanced(address,uint64,uint64,uint128,uint128,uint128,uint128)"));
    let config = addresses();
    let result = decode_invalidation(config.pool, log).unwrap();
    assert_eq!(result.positions, BTreeSet::from([
        key(config.market0.parse().unwrap(), config.borrower_a),
        key(config.market1.parse().unwrap(), config.borrower_a),
    ]));
    assert_eq!(invalidations("refinanced").positions, result.positions);
}

#[test]
fn real_repayment_refreshes_borrower_instead_of_other_payer() {
    let result = invalidations("repaid-other-payer");
    assert_eq!(result.positions, BTreeSet::from([borrower_market0()]));
    assert!(!result.positions.contains(&key(31_536_001, addresses().provider_b)));
}

#[test]
fn real_collateral_move_covers_zero_and_arbitrary_market() {
    let result = invalidations("collateral");
    assert_eq!(result.positions, BTreeSet::from([
        key(0, addresses().borrower_a),
        key(18_446_744_073_709_551_515, addresses().borrower_a),
    ]));
    assert_eq!(result.markets, BTreeSet::from([0, 18_446_744_073_709_551_515]));
    assert!(!result.positions.contains(&key(0, addresses().provider_c)));
}

#[test]
fn real_offset_and_liquidation_bad_debt_refresh_same_borrower() {
    for name in ["offset", "liquidated-bad-debt"] {
        let result = invalidations(name);
        assert_eq!(result.positions, BTreeSet::from([borrower_market0()]), "{name}");
        assert_eq!(result.markets, BTreeSet::from([31_536_001]), "{name}");
    }
    let logs = raw_logs(fixture("liquidated-bad-debt"));
    assert!(logs.iter().any(|log| log.topics[0] == keccak256("Liquidated(address,address,uint64,uint128,uint128)")));
    assert!(logs.iter().any(|log| log.topics[0] == keccak256("BadDebtIncurred(address,uint64,uint128)")));
}

#[test]
fn real_admin_changes_invalidate_config_without_fake_position() {
    let result = invalidations("admin");
    assert!(result.config_changed);
    assert!(result.positions.is_empty());
    assert!(result.markets.is_empty());
}

#[test]
fn malformed_dynamic_event_and_trailing_bytes_are_rejected() {
    let original = raw_logs(fixture("provided-borrow")).remove(0);
    let mut truncated = original.clone();
    truncated.data = truncated.data[..truncated.data.len() - 32].to_vec().into();
    assert!(matches!(decode_invalidation(addresses().pool, &truncated), Err(Error::Decode(_))));
    let mut trailing = original.clone();
    let mut data = trailing.data.to_vec();
    data.extend_from_slice(&[0_u8; 32]);
    trailing.data = data.into();
    assert!(matches!(decode_invalidation(addresses().pool, &trailing), Err(Error::Decode(_))));
    let mut offset = original;
    let mut data = offset.data.to_vec();
    data[2 * 32 + 31] = 0xa1;
    offset.data = data.into();
    assert!(matches!(decode_invalidation(addresses().pool, &offset), Err(Error::Decode(_))));
}

#[test]
fn dirty_indexed_padding_extra_topics_and_unknown_signatures_are_rejected() {
    let original = raw_logs(fixture("borrowed")).remove(0);
    let mut dirty_address = original.clone();
    let mut topic = dirty_address.topics[1].0;
    topic[0] = 1;
    dirty_address.topics[1] = B256::from(topic);
    assert!(matches!(decode_invalidation(addresses().pool, &dirty_address), Err(Error::Decode(_))));
    let mut dirty_market = original.clone();
    let mut topic = dirty_market.topics[2].0;
    topic[0] = 1;
    dirty_market.topics[2] = B256::from(topic);
    assert!(matches!(decode_invalidation(addresses().pool, &dirty_market), Err(Error::Decode(_))));
    let mut extra = original.clone();
    extra.topics.push(B256::ZERO);
    assert!(matches!(decode_invalidation(addresses().pool, &extra), Err(Error::Decode(_))));
    let mut unknown = original;
    unknown.topics[0] = keccak256("NotAProtocolEvent(uint128)");
    assert!(matches!(decode_invalidation(addresses().pool, &unknown), Err(Error::Decode(_))));
}

#[test]
fn normalize_sorts_deduplicates_and_rejects_conflicting_copies() {
    let logs = raw_logs(fixture("refinanced"));
    let mut duplicated = logs.clone();
    duplicated.reverse();
    duplicated.push(logs[0].clone());
    assert_eq!(normalize_logs(addresses().pool, 1, 1, duplicated).unwrap(), logs);
    let mut conflict = logs.clone();
    let mut changed = logs[0].clone();
    changed.data = Bytes::from(vec![0]);
    conflict.push(changed);
    assert!(matches!(normalize_logs(addresses().pool, 1, 1, conflict), Err(Error::Decode(_))));
}

#[test]
fn normalize_rejects_removed_wrong_address_range_and_fork_hash() {
    let log = raw_logs(fixture("borrowed")).remove(0);
    let mut removed = log.clone();
    removed.removed = true;
    assert!(matches!(normalize_logs(addresses().pool, 1, 1, vec![removed]), Err(Error::NonCanonical(1))));
    let mut wrong_address = log.clone();
    wrong_address.address = addresses().provider_b;
    assert!(matches!(normalize_logs(addresses().pool, 1, 1, vec![wrong_address]), Err(Error::Decode(_))));
    assert!(matches!(normalize_logs(addresses().pool, 2, 3, vec![log.clone()]), Err(Error::Decode(_))));
    let mut another = log.clone();
    another.log_index = 1;
    another.block_hash = B256::repeat_byte(0x44);
    assert!(matches!(normalize_logs(addresses().pool, 1, 1, vec![log, another]), Err(Error::NonCanonical(1))));
}
