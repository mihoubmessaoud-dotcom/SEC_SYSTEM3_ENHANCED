from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineResult:
    ticker: str = ""
    sector: str = ""
    sub_sector: str = ""
    status: str = "OK"
    reason: str = ""
    valid_years: list = field(default_factory=list)
    blocked_years: list = field(default_factory=list)
    ratios: dict = field(default_factory=dict)
    strategic: dict = field(default_factory=dict)
    verdicts: dict = field(default_factory=dict)
    quality_score: int = 0
    quality_verdict: str = ""
    professional_score: float = 0.0
    scoring_years: list = field(default_factory=list)
    display_years: list = field(default_factory=list)
    year_quality: dict = field(default_factory=dict)
    data_quality_grade: str = ""
    financial_health_indicator: str = ""
    peers: dict = field(default_factory=dict)
    forecasts: dict = field(default_factory=dict)
    audit: Optional[object] = None
    correction_alert: Optional[dict] = None
