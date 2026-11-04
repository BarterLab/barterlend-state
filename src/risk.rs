//! Protocol v5 risk calculations with checked arithmetic and Solidity rounding.

use alloy_primitives::{U256, U512};

use crate::types::{
    MarketRiskConfig, OracleFailure, OracleStatus, PoolRiskConfig, PositionAmounts,
    PositionRisk, Price, RiskFailure, RiskStatus,
};

/// Fixed-point scale for HF, LTV and discount: `1e18`.
pub const UNIT: u128 = 1_000_000_000_000_000_000;

/// Applies protocol price checks. A price is stale at the exact stale-window boundary;
/// v5 accepts future `updatedAt` values.
pub fn validate_price(
    price: &Price,
    global: &PoolRiskConfig,
    block_timestamp: u64,
) -> Result<(), OracleFailure> {
    if price.numerator == U256::ZERO {
        return Err(OracleFailure::ZeroNumerator);
    }
    if price.denominator == U256::ZERO {
        return Err(OracleFailure::ZeroDenominator);
    }
    if price.numerator / price.denominator > U256::from(u128::MAX) {
        return Err(OracleFailure::RatioTooLarge);
    }
    let fresh_until = price.updated_at.checked_add(U256::from(global.stale_window))
        .ok_or(OracleFailure::TimestampOverflow)?;
    if fresh_until <= U256::from(block_timestamp) {
        return Err(OracleFailure::Stale {
            updated_at: price.updated_at,
            stale_window: global.stale_window,
            block_timestamp,
        });
    }
    Ok(())
}

/// Validates ready prices while preserving typed transport/revert failures.
pub fn validate_oracle_status(
    oracle: &OracleStatus,
    global: &PoolRiskConfig,
    block_timestamp: u64,
) -> OracleStatus {
    match oracle {
        OracleStatus::Ready(price) => match validate_price(price, global, block_timestamp) {
            Ok(()) => OracleStatus::Ready(*price),
            Err(failure) => OracleStatus::Unavailable(failure),
        },
        OracleStatus::Unavailable(failure) => OracleStatus::Unavailable(failure.clone()),
    }
}

/// Calculates HF, liquidation eligibility and discount at a chain timestamp.
/// Amounts use token base units; the price ratio already includes token decimals.
pub fn evaluate_risk(
    amounts: &PositionAmounts,
    market: u64,
    global: &PoolRiskConfig,
    market_config: &MarketRiskConfig,
    oracle: &OracleStatus,
    block_timestamp: u64,
) -> RiskStatus {
    match evaluate(amounts, market, global, market_config, oracle, block_timestamp) {
        Ok(risk) => RiskStatus::Ready(risk),
        Err(failure) => RiskStatus::Unavailable(failure),
    }
}

fn evaluate(
    amounts: &PositionAmounts,
    market: u64,
    global: &PoolRiskConfig,
    market_config: &MarketRiskConfig,
    oracle: &OracleStatus,
    block_timestamp: u64,
) -> Result<PositionRisk, RiskFailure> {
    let unit = U256::from(UNIT);
    if global.liquidation_ltv > unit {
        return Err(RiskFailure::InvalidConfig("liquidation LTV exceeds UNIT".into()));
    }
    if market_config.discount_min > market_config.discount_max || market_config.discount_max >= unit {
        return Err(RiskFailure::InvalidConfig("invalid liquidation discount range".into()));
    }

    // debtOf/getHealthFactor short-circuit without accessing the oracle.
    if amounts.debt == 0 {
        return Ok(PositionRisk {
            health_factor: unit,
            eligible: false,
            discount: market_config.discount_min,
            has_collateral: amounts.collateral > 0,
            execution_ready: false,
        });
    }
    if amounts.collateral == 0 {
        return Ok(PositionRisk {
            health_factor: U256::ZERO,
            eligible: true,
            discount: market_config.discount_max,
            has_collateral: false,
            execution_ready: false,
        });
    }

    let price = match oracle {
        OracleStatus::Ready(price) => {
            validate_price(price, global, block_timestamp).map_err(RiskFailure::Oracle)?;
            price
        },
        OracleStatus::Unavailable(failure) => return Err(RiskFailure::Oracle(failure.clone())),
    };
    let collateral_value = mul_div_floor(
        U256::from(amounts.collateral), price.numerator, price.denominator,
    )?;
    let health_factor = mul_div_floor(global.liquidation_ltv, collateral_value, U256::from(amounts.debt))?;
    let liquidatable_after = U256::from(market) + U256::from(market_config.grace_period);
    let eligible = health_factor < unit || U256::from(block_timestamp) >= liquidatable_after;

    // Solidity checked left-to-right addition: min + UNIT, then - min(HF, UNIT).
    let discount_base = market_config.discount_min.checked_add(unit)
        .ok_or_else(|| RiskFailure::Arithmetic("liquidation discount overflow".into()))?;
    let discount = market_config.discount_max.min(discount_base - health_factor.min(unit));

    Ok(PositionRisk {
        health_factor,
        eligible,
        discount,
        has_collateral: true,
        execution_ready: eligible,
    })
}

fn mul_div_floor(left: U256, right: U256, denominator: U256) -> Result<U256, RiskFailure> {
    if denominator == U256::ZERO {
        return Err(RiskFailure::Arithmetic("mulDiv denominator is zero".into()));
    }
    let product = U512::from(left) * U512::from(right);
    let quotient = product / U512::from(denominator);
    if quotient > U512::from(U256::MAX) {
        return Err(RiskFailure::Arithmetic("mulDiv result exceeds uint256".into()));
    }
    let limbs = quotient.as_limbs();
    Ok(U256::from_limbs([limbs[0], limbs[1], limbs[2], limbs[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn globals() -> PoolRiskConfig {
        PoolRiskConfig {
            liquidation_ltv: U256::from(UNIT * 8 / 10),
            stale_window: 10,
            min_debt_remaining: 0,
        }
    }

    fn market_config() -> MarketRiskConfig {
        MarketRiskConfig {
            grace_period: 10,
            discount_min: U256::from(UNIT / 100),
            discount_max: U256::from(UNIT * 8 / 100),
        }
    }

    fn oracle() -> OracleStatus {
        OracleStatus::Ready(Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(100) })
    }

    fn ready(result: RiskStatus) -> PositionRisk {
        match result {
            RiskStatus::Ready(risk) => risk,
            other => panic!("expected ready risk, got {other:?}"),
        }
    }

    #[test]
    fn health_factor_equal_unit_is_not_health_liquidation() {
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 80, collateral: 100 }, 200, &globals(), &market_config(), &oracle(), 100));
        assert_eq!(risk.health_factor, U256::from(UNIT));
        assert_eq!(risk.discount, U256::from(UNIT / 100));
        assert!(!risk.eligible);
        assert!(!risk.execution_ready);
    }

    #[test]
    fn discount_increases_and_caps_below_unit_health() {
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 100, collateral: 100 }, 200, &globals(), &market_config(), &oracle(), 100));
        assert_eq!(risk.health_factor, U256::from(800_000_000_000_000_000_u128));
        assert_eq!(risk.discount, U256::from(80_000_000_000_000_000_u128));
        assert!(risk.eligible);
        assert!(risk.execution_ready);
    }

    #[test]
    fn discount_keeps_uncapped_health_delta() {
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 100, collateral: 122 }, 200, &globals(), &market_config(), &oracle(), 100));
        assert_eq!(risk.health_factor, U256::from(976_000_000_000_000_000_u128));
        assert_eq!(risk.discount, U256::from(34_000_000_000_000_000_u128));
    }

    #[test]
    fn maturity_grace_equality_allows_healthy_liquidation() {
        let before = ready(evaluate_risk(&PositionAmounts { debt: 50, collateral: 100 }, 100, &globals(), &market_config(), &OracleStatus::Ready(Price { updated_at: U256::from(110), numerator: U256::from(1), denominator: U256::from(1) }), 109));
        let at = ready(evaluate_risk(&PositionAmounts { debt: 50, collateral: 100 }, 100, &globals(), &market_config(), &OracleStatus::Ready(Price { updated_at: U256::from(110), numerator: U256::from(1), denominator: U256::from(1) }), 110));
        assert!(!before.eligible);
        assert!(at.eligible);
        assert_eq!(at.health_factor, U256::from(1_600_000_000_000_000_000_u128));
        assert_eq!(at.discount, U256::from(UNIT / 100));
    }

    #[test]
    fn collateral_and_health_value_round_down_separately() {
        let price = OracleStatus::Ready(Price { numerator: U256::from(2), denominator: U256::from(3), updated_at: U256::from(100) });
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 3, collateral: 5 }, 200, &globals(), &market_config(), &price, 100));
        assert_eq!(risk.health_factor, U256::from(800_000_000_000_000_000_u128));
    }

    #[test]
    fn price_freshness_equality_is_stale() {
        let price = match oracle() { OracleStatus::Ready(price) => price, _ => unreachable!() };
        assert_eq!(validate_price(&price, &globals(), 109), Ok(()));
        assert_eq!(validate_price(&price, &globals(), 110), Err(OracleFailure::Stale { updated_at: U256::from(100), stale_window: 10, block_timestamp: 110 }));
    }

    #[test]
    fn future_updated_at_matches_protocol_acceptance() {
        let price = Price { numerator: U256::from(1), denominator: U256::from(1), updated_at: U256::from(200) };
        assert_eq!(validate_price(&price, &globals(), 100), Ok(()));
    }

    #[test]
    fn price_ratio_limit_uses_integer_division_like_protocol() {
        let price = Price {
            numerator: U256::from(u128::MAX) * U256::from(2) + U256::from(1),
            denominator: U256::from(2),
            updated_at: U256::from(100),
        };
        assert_eq!(validate_price(&price, &globals(), 100), Ok(()));
    }

    #[test]
    fn invalid_prices_and_timestamp_overflow_are_typed() {
        let mut price = Price { numerator: U256::ZERO, denominator: U256::from(1), updated_at: U256::from(100) };
        assert_eq!(validate_price(&price, &globals(), 100), Err(OracleFailure::ZeroNumerator));
        price.numerator = U256::from(1);
        price.denominator = U256::ZERO;
        assert_eq!(validate_price(&price, &globals(), 100), Err(OracleFailure::ZeroDenominator));
        price.numerator = U256::from(u128::MAX) + U256::from(1);
        price.denominator = U256::from(1);
        assert_eq!(validate_price(&price, &globals(), 100), Err(OracleFailure::RatioTooLarge));
        price.numerator = U256::from(1);
        price.updated_at = U256::MAX;
        assert_eq!(validate_price(&price, &globals(), 100), Err(OracleFailure::TimestampOverflow));
    }

    #[test]
    fn zero_debt_and_collateral_do_not_depend_on_oracle() {
        let unavailable = OracleStatus::Unavailable(OracleFailure::Rpc("offline".into()));
        let no_debt = ready(evaluate_risk(&PositionAmounts { debt: 0, collateral: 100 }, 200, &globals(), &market_config(), &unavailable, 100));
        assert_eq!(no_debt.health_factor, U256::from(UNIT));
        assert!(!no_debt.eligible);
        assert!(!no_debt.execution_ready);
        assert!(no_debt.has_collateral);
        let no_collateral = ready(evaluate_risk(&PositionAmounts { debt: 100, collateral: 0 }, 200, &globals(), &market_config(), &unavailable, 100));
        assert_eq!(no_collateral.health_factor, U256::ZERO);
        assert!(no_collateral.eligible);
        assert!(!no_collateral.execution_ready);
        assert!(!no_collateral.has_collateral);
    }

    #[test]
    fn positive_debt_and_collateral_preserve_oracle_failure() {
        let unavailable = OracleStatus::Unavailable(OracleFailure::Rpc("offline".into()));
        assert_eq!(evaluate_risk(&PositionAmounts { debt: 100, collateral: 100 }, 200, &globals(), &market_config(), &unavailable, 100), RiskStatus::Unavailable(RiskFailure::Oracle(OracleFailure::Rpc("offline".into()))));
    }

    #[test]
    fn decimals_are_already_in_price_fraction() {
        let price = OracleStatus::Ready(Price { numerator: U256::from(2_000_000_000_u64), denominator: U256::from(1_000_000_000_000_000_000_u64), updated_at: U256::from(100) });
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 1_600_000_000, collateral: 1_000_000_000_000_000_000 }, 200, &globals(), &market_config(), &price, 100));
        assert_eq!(risk.health_factor, U256::from(UNIT));
        let reverse_price = OracleStatus::Ready(Price { numerator: U256::from(1_000_000_000_000_000_000_u64), denominator: U256::from(1_000_000_u64), updated_at: U256::from(100) });
        let reverse = ready(evaluate_risk(&PositionAmounts { debt: 800_000_000_000_000_000, collateral: 1_000_000 }, 200, &globals(), &market_config(), &reverse_price, 100));
        assert_eq!(reverse.health_factor, U256::from(UNIT));
        let eight_to_six_price = OracleStatus::Ready(Price { numerator: U256::from(2_000_000_000_u64), denominator: U256::from(100_000_000_u64), updated_at: U256::from(100) });
        let eight_to_six = ready(evaluate_risk(&PositionAmounts { debt: 1_600_000_000, collateral: 100_000_000 }, 200, &globals(), &market_config(), &eight_to_six_price, 100));
        assert_eq!(eight_to_six.health_factor, U256::from(UNIT));
    }

    #[test]
    fn oracle_validation_keeps_failure_or_classifies_stale_price() {
        let failure = OracleFailure::Decode("malformed price".into());
        assert_eq!(validate_oracle_status(&OracleStatus::Unavailable(failure.clone()), &globals(), 100), OracleStatus::Unavailable(failure));
        assert_eq!(validate_oracle_status(&oracle(), &globals(), 110), OracleStatus::Unavailable(OracleFailure::Stale { updated_at: U256::from(100), stale_window: 10, block_timestamp: 110 }));
    }

    #[test]
    fn mul_div_allows_uint256_product_overflow_when_result_fits() {
        assert_eq!(mul_div_floor(U256::MAX, U256::MAX, U256::MAX).unwrap(), U256::MAX);
        let price = OracleStatus::Ready(Price { numerator: U256::MAX, denominator: U256::MAX, updated_at: U256::from(100) });
        let risk = ready(evaluate_risk(&PositionAmounts { debt: u128::MAX, collateral: u128::MAX }, 200, &globals(), &market_config(), &price, 100));
        assert_eq!(risk.health_factor, U256::from(800_000_000_000_000_000_u128));
    }

    #[test]
    fn mul_div_rejects_result_overflow_and_zero_denominator() {
        assert!(matches!(mul_div_floor(U256::MAX, U256::MAX, U256::from(1)), Err(RiskFailure::Arithmetic(_))));
        assert!(matches!(mul_div_floor(U256::from(1), U256::from(1), U256::ZERO), Err(RiskFailure::Arithmetic(_))));
    }

    #[test]
    fn overflowing_health_result_is_not_saturated() {
        let price = OracleStatus::Ready(Price { numerator: U256::from(u128::MAX), denominator: U256::from(1), updated_at: U256::from(100) });
        assert!(matches!(evaluate_risk(&PositionAmounts { debt: 1, collateral: u128::MAX }, 200, &globals(), &market_config(), &price, 100), RiskStatus::Unavailable(RiskFailure::Arithmetic(_))));
    }

    #[test]
    fn large_maturity_does_not_wrap_u64_grace_sum() {
        let mut price = match oracle() { OracleStatus::Ready(price) => price, _ => unreachable!() };
        price.updated_at = U256::from(u64::MAX);
        let risk = ready(evaluate_risk(&PositionAmounts { debt: 50, collateral: 100 }, u64::MAX, &globals(), &market_config(), &OracleStatus::Ready(price), u64::MAX));
        assert!(!risk.eligible);
    }

    #[test]
    fn invalid_config_is_explicit() {
        let mut config = market_config();
        config.discount_max = U256::from(UNIT);
        assert!(matches!(evaluate_risk(&PositionAmounts { debt: 80, collateral: 100 }, 200, &globals(), &config, &oracle(), 100), RiskStatus::Unavailable(RiskFailure::InvalidConfig(_))));
    }
}
