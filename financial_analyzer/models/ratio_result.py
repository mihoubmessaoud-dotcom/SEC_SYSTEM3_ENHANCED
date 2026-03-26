from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class RatioResult:
    value: Optional[float]
    status: str  # COMPUTED | NOT_COMPUTABLE | OUTLIER
    reliability: int  # 0-100
    source: str = ""
    reason: str = ""  # سبب N/A إذا وُجد
    flag: str = ""  # EXTREME_ROE_BUYBACK | INFERRED | ...
    display: str = ""  # النص المعروض للمستخدم
    note: str = ""  # شرح للمستخدم
    inputs_used: list = field(default_factory=list)
    # Optional metric identifier for debug/telemetry compatibility.
    metric: str = ""

    def is_valid(self):
        return self.value is not None and self.status == "COMPUTED"

    def to_display(self, format_type="percent"):
        if self.value is None:
            reason_ar = {
                "ZERO_DENOMINATOR": "مقام صفر",
                "MISSING_INPUT": "بيانات مفقودة",
                "MISSING_SEC_CONCEPT": "مفهوم SEC غير متاح",
                "BLOCKED_BY_SECTOR": "غير مطبَّق لهذا القطاع",
                "ANCHOR_MISSING": "مرساة الميزانية مفقودة",
                "IMPOSSIBLE_VALUE": "قيمة مستحيلة",
                "MISSING_CCC_COMPONENTS": "مكونات CCC ناقصة",
            }.get(self.reason, self.reason)
            if reason_ar:
                return f"— ({reason_ar}) ⚠️"
            return "—"

        if format_type == "percent":
            base = f"{self.value:.1%}"
        elif format_type == "multiple":
            base = f"{self.value:.1f}×"
        elif format_type == "days":
            base = f"{self.value:.0f} يوم"
        else:
            base = f"{self.value:.2f}"

        if self.flag or self.status not in ("COMPUTED", ""):
            return f"{base} ⚠️"
        return base
