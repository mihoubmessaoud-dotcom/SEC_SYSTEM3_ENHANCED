from dataclasses import dataclass, field


@dataclass
class CompanyData:
    ticker: str = ""
    sector: str = ""
    sub_sector: str = ""
    sic_code: str = ""
    years: list = field(default_factory=list)
    raw_by_year: dict = field(default_factory=dict)
    resolved_by_year: dict = field(default_factory=dict)
    market_by_year: dict = field(default_factory=dict)
    ratios_by_year: dict = field(default_factory=dict)
    strategic_by_year: dict = field(default_factory=dict)
