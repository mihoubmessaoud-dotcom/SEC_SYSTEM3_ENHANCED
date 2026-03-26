from modules.data_repository import DataRepository
from modules.ratio_engine_cached import RatioEngine, RatioRegistry


def main() -> None:
    repo = DataRepository()
    registry = RatioRegistry()
    engine = RatioEngine(repository=repo, registry=registry)

    # Clean values are pre-validated and stored once.
    repo.set_clean("operating_income:2025", 152_000, reason="validated")
    repo.set_clean("tax_rate:2025", 0.18, reason="validated")
    repo.set_clean("invested_capital:2025", 620_000, reason="validated")
    repo.set_clean("net_income:2025", 112_000, reason="validated")
    repo.set_clean("total_equity:2025", 79_000, reason="validated")
    repo.set_clean("pe_ratio:2025", 31.2, reason="validated")
    repo.set_clean("earnings_growth:2025", 0.156, reason="validated")  # 15.6%
    repo.set_clean("gross_profit:2025", 180_000, reason="validated")
    repo.set_clean("revenue:2025", 430_000, reason="validated")

    # First call computes + caches.
    print(engine.calculate("roic", 2025))
    print(engine.calculate("roe", 2025))
    print(engine.calculate("peg", 2025))
    print(engine.calculate("gross_margin", 2025))

    # Second call returns cached without recalculation.
    print(engine.calculate("roic", 2025))
    print("roic_compute_count:", engine.get_compute_count("roic"))


if __name__ == "__main__":
    main()
