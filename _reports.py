import json, time, os, sys
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class RunReport:
    brand: str
    models: int = 0
    versions: int = 0
    items: int = 0
    errors: int = 0
    duration_sec: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    meta: Optional[Dict[str, Any]] = None  # extras (ej: source urls, warnings, etc)

class ReportTimer:
    def __init__(self, brand: str):
        self.brand = brand
        self.t0 = time.time()
        self.report = RunReport(brand=brand, started_at=self.t0)

    def finalize(self, **kwargs):
        t1 = time.time()
        self.report.finished_at = t1
        self.report.duration_sec = round(t1 - self.t0, 3)
        for k, v in kwargs.items():
            if hasattr(self.report, k):
                setattr(self.report, k, v)
            else:
                if self.report.meta is None:
                    self.report.meta = {}
                self.report.meta[k] = v

    def print_result_line(self):
        payload = asdict(self.report)
        print("__RESULT__=" + json.dumps(payload, ensure_ascii=False), flush=True)
