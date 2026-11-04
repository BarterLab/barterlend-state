//! HF and eligibility expectations were recorded from CreditPool getters in Foundry.

use alloy_primitives::U256;
use barterlend_state::{MarketRiskConfig, OracleFailure, OracleStatus, PoolRiskConfig, PositionAmounts, Price, RiskFailure, RiskStatus, risk::{UNIT, evaluate_risk}};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RiskVector {
    name: String,
    market: String,
    timestamp: String,
    debt: String,
    collateral: String,
    price_numerator: String,
    price_denominator: String,
    updated_at: String,
    stale_window: String,
    liquidation_ltv: String,
    grace_period: String,
    discount_min: String,
    discount_max: String,
    min_debt_remaining: String,
    health_factor_call_ok: bool,
    health_factor: String,
    eligible_call_ok: bool,
    eligible: Option<bool>,
    revert_data: String,
}

fn uint(text: &str) -> U256 {
    U256::from_str_radix(text, 10).unwrap()
}

fn vectors() -> Vec<RiskVector> {
    serde_json::from_str(include_str!("fixtures/protocol-v5/generated/risk-vectors.json")).unwrap()
}

fn evaluate(vector: &RiskVector) -> RiskStatus {
    evaluate_risk(
        &PositionAmounts { debt: vector.debt.parse().unwrap(), collateral: vector.collateral.parse().unwrap() },
        vector.market.parse().unwrap(),
        &PoolRiskConfig {
            liquidation_ltv: uint(&vector.liquidation_ltv),
            stale_window: vector.stale_window.parse().unwrap(),
            min_debt_remaining: vector.min_debt_remaining.parse().unwrap(),
        },
        &MarketRiskConfig {
            grace_period: vector.grace_period.parse().unwrap(),
            discount_min: uint(&vector.discount_min),
            discount_max: uint(&vector.discount_max),
        },
        &OracleStatus::Ready(Price {
            numerator: uint(&vector.price_numerator),
            denominator: uint(&vector.price_denominator),
            updated_at: uint(&vector.updated_at),
        }),
        vector.timestamp.parse().unwrap(),
    )
}

fn compare_solidity(vector: &RiskVector) {
    match evaluate(vector) {
        RiskStatus::Ready(risk) => {
            assert!(vector.health_factor_call_ok, "{} Solidity HF failed", vector.name);
            assert!(vector.eligible_call_ok, "{} Solidity eligibility failed", vector.name);
            assert_eq!(risk.health_factor, uint(&vector.health_factor), "{} HF", vector.name);
            assert_eq!(Some(risk.eligible), vector.eligible, "{} eligibility", vector.name);
            assert_eq!(risk.has_collateral, vector.collateral != "0", "{} collateral", vector.name);
            assert_eq!(risk.execution_ready, risk.eligible && vector.collateral != "0" && vector.debt != "0", "{} risk readiness", vector.name);
        },
        RiskStatus::Unavailable(RiskFailure::Oracle(OracleFailure::Stale { updated_at, stale_window, block_timestamp })) => {
            assert_eq!(vector.name, "stale-equality");
            assert!(!vector.health_factor_call_ok);
            assert!(!vector.eligible_call_ok);
            assert_eq!(vector.eligible, None);
            assert!(vector.revert_data.starts_with("0x76d4f6cd"));
            assert_eq!(updated_at, uint(&vector.updated_at));
            assert_eq!(stale_window, vector.stale_window.parse::<u64>().unwrap());
            assert_eq!(block_timestamp, vector.timestamp.parse::<u64>().unwrap());
        },
        result => panic!("{} unexpected SDK result: {result:?}", vector.name),
    }
}

#[test]
fn risk_matches_actual_solidity_getters_for_all_reference_vectors() {
    let vectors = vectors();
    assert!(vectors.len() >= 9);
    for vector in &vectors {
        compare_solidity(vector);
    }
}

#[test]
fn risk_matches_post_action_solidity_getters() {
    for json in [
        include_str!("fixtures/protocol-v5/generated/borrowed.json"),
        include_str!("fixtures/protocol-v5/generated/provided-borrow.json"),
        include_str!("fixtures/protocol-v5/generated/provided-mixed.json"),
        include_str!("fixtures/protocol-v5/generated/provided-all-arrays.json"),
        include_str!("fixtures/protocol-v5/generated/sweep.json"),
        include_str!("fixtures/protocol-v5/generated/refinanced.json"),
        include_str!("fixtures/protocol-v5/generated/repaid-other-payer.json"),
        include_str!("fixtures/protocol-v5/generated/collateral.json"),
        include_str!("fixtures/protocol-v5/generated/offset.json"),
        include_str!("fixtures/protocol-v5/generated/liquidated-bad-debt.json"),
        include_str!("fixtures/protocol-v5/generated/admin.json"),
    ] {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let vector: RiskVector = serde_json::from_value(value["risk"].clone()).unwrap();
        compare_solidity(&vector);
    }
}

#[test]
fn real_boundary_vectors_preserve_strict_thresholds_and_discount_rounding() {
    let vectors = vectors();
    let named = |name: &str| vectors.iter().find(|vector| vector.name == name).unwrap();
    let ready = |name: &str| match evaluate(named(name)) {
        RiskStatus::Ready(risk) => risk,
        result => panic!("{name}: {result:?}"),
    };
    assert_eq!(ready("hf-equality").health_factor, U256::from(UNIT));
    assert!(!ready("hf-equality").eligible);
    assert_eq!(ready("hf-below").health_factor, U256::from(999_999_999_999_999_998_u128));
    assert!(ready("hf-below").eligible);
    assert_eq!(ready("hf-below").discount, U256::from(50_000_000_000_000_002_u128));
    assert!(!ready("grace-before").eligible);
    assert!(ready("grace-equality").eligible);
    assert_eq!(ready("grace-equality").discount, U256::from(50_000_000_000_000_000_u128));
    assert!(ready("fresh-before").eligible);
    assert!(matches!(evaluate(named("stale-equality")), RiskStatus::Unavailable(RiskFailure::Oracle(OracleFailure::Stale { .. }))));
    assert_eq!(ready("fractional-price").health_factor, U256::ZERO);
    assert_eq!(ready("fractional-price").discount, U256::from(200_000_000_000_000_000_u128));
}

#[test]
#[ignore = "fresh isolated Solidity generator: bash tests/runners/test-decimal-risk.sh /absolute/new-evidence-directory"]
fn risk_matches_fresh_decimal_solidity_getters() {
    #[derive(Deserialize)]
    struct DecimalVector { source_decimals: u8, target_decimals: u8, risk: RiskVector }
    let path = std::env::var("BARTERLEND_DECIMAL_RISK_VECTORS").expect("fresh Solidity getter output is required");
    let text = std::fs::read_to_string(path).unwrap();
    let vectors: Vec<DecimalVector> = serde_json::from_str(&text).unwrap();
    assert_eq!(vectors.len(), 24);
    let mut seen = std::collections::BTreeSet::new();
    for vector in &vectors {
        assert!(seen.insert((vector.source_decimals, vector.target_decimals, vector.risk.name.clone())), "duplicate vector");
        compare_solidity(&vector.risk);
    }
    for pair in [(18, 6), (6, 18), (8, 6), (6, 8)] {
        for name in ["healthy", "hf-equality", "hf-below", "double-floor", "wide-intermediate", "maturity-equality"] {
            assert!(seen.contains(&(pair.0, pair.1, name.to_owned())), "missing actual decimal pair/boundary {pair:?}/{name}");
        }
    }
    println!("compared {} fresh actual Solidity getter vectors", vectors.len());
}
