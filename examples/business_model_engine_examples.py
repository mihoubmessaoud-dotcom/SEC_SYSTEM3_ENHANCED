from modules.business_model_engine import BusinessModelEngine


def main() -> None:
    engine = BusinessModelEngine()

    nvda = engine.classify(
        gross_margin=0.75,
        capex_to_revenue=0.04,
        rd_to_revenue=0.15,
    )
    amd = engine.classify(
        gross_margin=0.49,
        capex_to_revenue=0.07,
        rd_to_revenue=0.24,
    )
    intc = engine.classify(
        gross_margin=0.35,
        capex_to_revenue=0.28,
        rd_to_revenue=0.14,
    )

    print("NVDA:", nvda)
    print("AMD :", amd)
    print("INTC:", intc)


if __name__ == "__main__":
    main()
