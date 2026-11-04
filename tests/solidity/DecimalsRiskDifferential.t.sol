// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.29;

import { CreditPoolTestSuite, MockCreditPool } from "test/CreditPool/0_base.t.sol";
import { MockToken } from "test/mocks/MockToken.sol";
import { MockPriceOracle } from "test/mocks/MockPriceOracle.sol";
import { MockAaveToken } from "test/mocks/MockAaveToken.sol";
import { MockAavePool } from "test/mocks/MockAavePool.sol";
import { AaveV3Adapter } from "src/adapters/AaveV3Adapter.sol";
import { UnderlyingPoolConfig } from "src/interfaces/IUnderlyingPool.sol";
import { MarketsConfigParams } from "src/lib/MarketsConfig.sol";
import { CreditPoolFacet1 } from "src/facets/CreditPoolFacet1.sol";
import { CreditPoolFacet2 } from "src/facets/CreditPoolFacet2.sol";
import { CreditPoolFacet3 } from "src/facets/CreditPoolFacet3.sol";
import { CreditPoolFacet4 } from "src/facets/CreditPoolFacet4.sol";

contract DecimalsToken is MockToken {
    uint8 internal immutable configuredDecimals;
    constructor(uint8 value) { configuredDecimals = value; }
    function decimals() public view override returns (uint8) { return configuredDecimals; }
}

// Only the test setter is added: risk functions still delegate to the pinned, unmodified v5 facets.
contract DecimalsRiskPool is MockCreditPool {
    constructor(InitializationParams memory params, address[] memory facets) MockCreditPool(params, facets) {}
    function setPosition(uint64 market, address borrower, uint128 debt, uint128 collateral_) external {
        _users[borrower].debt[market] = debt;
        _users[borrower].collateral[market] = collateral_;
    }
}

contract DecimalsRiskDifferential is CreditPoolTestSuite {
    address internal fixtureAdmin = makeAddr("Decimal Fixture Admin");

    function test_generate_decimal_risk_vectors() public {
        string memory json = "[";
        json = string.concat(json, _pair(18, 6), ",", _pair(6, 18), ",", _pair(8, 6), ",", _pair(6, 8), "]");
        vm.writeFile("../generated/decimal-risk-vectors.json", json);
    }

    function _pair(uint8 sourceDecimals, uint8 targetDecimals) internal returns (string memory json) {
        collateral = new DecimalsToken(sourceDecimals);
        asset = new DecimalsToken(targetDecimals);
        aaveToken = new MockAaveToken(asset, 10);
        aavePool = new MockAavePool(asset, aaveToken);
        aaveAdapter = new AaveV3Adapter();
        priceOracle = new MockPriceOracle(address(collateral), address(asset));
        address[] memory facets = new address[](4);
        facets[0] = address(new CreditPoolFacet1()); facets[1] = address(new CreditPoolFacet2());
        facets[2] = address(new CreditPoolFacet3()); facets[3] = address(new CreditPoolFacet4());
        uint128 targetUnit = uint128(10 ** targetDecimals);
        uint128 sourceUnit = uint128(10 ** sourceDecimals);
        InitializationParams memory params = InitializationParams({
            owner: fixtureAdmin, permit2: address(permit2), asset: address(asset),
            underlyingPool: UnderlyingPoolConfig(address(aaveAdapter), bytes32(uint256(uint160(address(aaveToken))))),
            collateral: address(collateral), priceOracle: address(priceOracle), stalePriceWindow: 24 hours,
            protocolFee: 0, totalCollateralCap: type(uint128).max, provideOrderMinAmount: targetUnit,
            borrowOrderMinAmount: targetUnit, maxActiveProvideOrders: 8, maxActiveBorrowOrders: 1,
            borrowLTV: UNIT / 2, liquidationLTV: UNIT / 2, liquidationDiscountMin: UNIT * 5 / 100,
            liquidationDiscountMax: UNIT / 5, minDebtRemainingAfterLiquidation: targetUnit * 10,
            liquidationGracePeriod: 1 days,
            marketsConfig: MarketsConfigParams([YEAR, YEAR, YEAR], [uint8(3), 3, 3], [uint64(0), 0, 0])
        });
        DecimalsRiskPool exposed = new DecimalsRiskPool(params, facets);
        pool = exposed;
        assertEq(collateral.decimals(), sourceDecimals); assertEq(asset.decimals(), targetDecimals);
        assertEq(pool.collateral(), address(collateral)); assertEq(pool.asset(), address(asset));
        assertEq(priceOracle.source(), address(collateral)); assertEq(priceOracle.target(), address(asset));

        exposed.setPosition(markets[0], borrower_A, 1000 * targetUnit, sourceUnit);
        // Human price 4000/2000 target tokens per source token; fractions already include both decimals.
        priceOracle.setPriceFraction(4000 * uint256(targetUnit), sourceUnit);
        assertGt(pool.getHealthFactor(markets[0], borrower_A), UNIT);
        json = _record("healthy", sourceDecimals, targetDecimals);
        priceOracle.setPriceFraction(2000 * uint256(targetUnit), sourceUnit);
        assertEq(pool.getHealthFactor(markets[0], borrower_A), UNIT);
        assertFalse(pool.isLiquidatable(markets[0], borrower_A));
        json = string.concat(json, ",", _record("hf-equality", sourceDecimals, targetDecimals));
        priceOracle.setPriceFraction(2000 * uint256(targetUnit) - 1, sourceUnit);
        assertTrue(pool.isLiquidatable(markets[0], borrower_A));
        json = string.concat(json, ",", _record("hf-below", sourceDecimals, targetDecimals));

        // Raw-unit adversarial fraction catches incorrectly merged floor operations.
        exposed.setPosition(markets[0], borrower_A, 1, 1);
        priceOracle.setPriceFraction(3, 2);
        assertEq(pool.getHealthFactor(markets[0], borrower_A), UNIT / 2);
        json = string.concat(json, ",", _record("double-floor", sourceDecimals, targetDecimals));

        // The numerator product exceeds uint256, while actual mulDiv result remains representable.
        exposed.setPosition(markets[0], borrower_A, type(uint128).max, type(uint128).max);
        priceOracle.setPriceFraction(uint256(1) << 190, uint256(1) << 190);
        assertEq(pool.getHealthFactor(markets[0], borrower_A), UNIT / 2);
        json = string.concat(json, ",", _record("wide-intermediate", sourceDecimals, targetDecimals));

        exposed.setPosition(markets[0], borrower_A, 1000 * targetUnit, sourceUnit);
        priceOracle.setPriceFraction(4000 * uint256(targetUnit), sourceUnit);
        vm.warp(uint256(markets[0]) + pool.getLiquidationGracePeriod(markets[0])); priceOracle.touch();
        assertTrue(pool.isLiquidatable(markets[0], borrower_A));
        json = string.concat(json, ",", _record("maturity-equality", sourceDecimals, targetDecimals));
        vm.warp(1);
    }

    function _record(string memory name, uint8 sourceDecimals, uint8 targetDecimals) internal view returns (string memory) {
        return string.concat('{"source_decimals":', vm.toString(sourceDecimals), ',"target_decimals":', vm.toString(targetDecimals),
            ',"risk":', _riskVector(name, borrower_A, markets[0]), '}');
    }

    function _riskVector(string memory name, address account, uint64 market) internal view returns (string memory) {
        (bool hfOk, bytes memory hf) = address(pool).staticcall(abi.encodeWithSignature("getHealthFactor(uint64,address)", market, account));
        (bool eligibleOk, bytes memory eligible) = address(pool).staticcall(abi.encodeWithSignature("isLiquidatable(uint64,address)", market, account));
        string memory json = string.concat('{"name":"', name, '","market":"', vm.toString(market), '","timestamp":"', vm.toString(block.timestamp),
            '","debt":"', vm.toString(pool.debtOf(market, account)), '","collateral":"', vm.toString(pool.collateralOf(market, account)));
        json = string.concat(json, '","price_numerator":"', vm.toString(priceOracle.price()), '","price_denominator":"', vm.toString(priceOracle.denominator()),
            '","updated_at":"', vm.toString(priceOracle.updatedAt()), '","stale_window":"', vm.toString(pool.getStalePriceWindow()));
        json = string.concat(json, '","liquidation_ltv":"', vm.toString(pool.getLiquidationLTV()), '","grace_period":"', vm.toString(pool.getLiquidationGracePeriod(market)),
            '","discount_min":"', vm.toString(pool.getLiquidationDiscountMin(market)), '","discount_max":"', vm.toString(pool.getLiquidationDiscountMax(market)),
            '","min_debt_remaining":"', vm.toString(pool.getMinDebtRemainingAfterLiquidation()));
        return string.concat(json, '","health_factor_call_ok":', hfOk ? "true" : "false", ',"health_factor":"', hfOk ? vm.toString(abi.decode(hf, (uint256))) : "",
            '","eligible_call_ok":', eligibleOk ? "true" : "false", ',"eligible":', eligibleOk ? (abi.decode(eligible, (bool)) ? "true" : "false") : "null",
            ',"revert_data":"', vm.toString(hfOk ? bytes("") : hf), '"}');
    }
}
