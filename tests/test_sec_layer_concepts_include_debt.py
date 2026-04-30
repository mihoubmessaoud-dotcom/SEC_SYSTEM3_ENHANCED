from layers.sec_layer import SECLayer


def test_sec_layer_concepts_include_debt_and_leases():
    layer = SECLayer(user_agent="test-agent")
    concepts = layer._concepts_for_statements()
    assert "us-gaap:TotalDebt" in concepts
    assert "us-gaap:DebtCurrent" in concepts
    assert "us-gaap:LongTermDebtNoncurrent" in concepts
    # Lease-related concepts should also be ingestible (policy handled upstream).
    assert "us-gaap:LongTermDebtAndCapitalLeaseObligations" in concepts
    assert "us-gaap:OperatingLeaseLiability" in concepts

