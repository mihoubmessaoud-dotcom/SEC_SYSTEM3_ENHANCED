from datetime import datetime


class AuditLog:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.entries = []
        self.flags = {}
        self.corrections = []

    def log(self, step: str, data: dict):
        self.entries.append(
            {
                "step": step,
                "timestamp": datetime.now().isoformat(),
                "data": data,
            }
        )

    def flag(
        self,
        year: int,
        flag_type: str,
        severity: str = "HIGH",
        note: str = "",
    ):
        if year not in self.flags:
            self.flags[year] = []
        self.flags[year].append(
            {
                "type": flag_type,
                "severity": severity,
                "note": note,
            }
        )

    def correction(
        self,
        year: int,
        field: str,
        old_val: object,
        new_val: object,
        reason: str,
    ):
        self.corrections.append(
            {
                "year": year,
                "field": field,
                "old_value": old_val,
                "new_value": new_val,
                "reason": reason,
            }
        )

    def get_flags(self, year: int):
        return self.flags.get(year, [])

    def summary(self):
        return {
            "total_corrections": len(self.corrections),
            "total_flags": sum(len(v) for v in self.flags.values()),
            "flagged_years": list(self.flags.keys()),
            "steps_completed": len(self.entries),
        }
