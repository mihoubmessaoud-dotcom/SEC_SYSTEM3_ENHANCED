from modules.data_repository import DataRepository
from modules.ratio_engine_cached import (
    DuplicateRatioCalculationError,
    RatioEngine,
    RatioRegistry,
)


def main() -> None:
    repo = DataRepository()
    reg = RatioRegistry()
    engine = RatioEngine(repository=repo, registry=reg)

    repo.set_clean("operating_income:2025", 152_000, reason="validated")
    repo.set_clean("depreciation_amortization:2025", 18_000, reason="validated")
    repo.set_clean("tax_rate:2025", 0.20, reason="validated")
    repo.set_clean("invested_capital:2025", 760_000, reason="validated")
    repo.set_clean("net_income:2025", 114_000, reason="validated")
    repo.set_clean("total_equity:2025", 95_000, reason="validated")
    repo.set_clean("pe_ratio:2025", 30.0, reason="validated")
    repo.set_clean("earnings_growth:2025", 0.15, reason="validated")

    print("EBITDA:", engine.calculate("ebitda", 2025))
    print("ROIC:", engine.calculate("roic", 2025))
    print("ROE:", engine.calculate("roe", 2025))
    print("PEG:", engine.calculate("peg", 2025))

    # Default mode returns cached.
    print("ROIC (cached):", engine.calculate("roic", 2025))

    # Strict mode raises on repeated request.
    try:
        engine.calculate("roic", 2025, strict_no_repeat=True)
    except DuplicateRatioCalculationError as exc:
        print("STRICT_BLOCKED:", str(exc))


if __name__ == "__main__":
    main()
