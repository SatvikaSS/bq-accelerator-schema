import json
import yaml


class DriftReportExporter:
    def __init__(self, diff_report: dict):
        self.report = diff_report

    def to_json(self, path: str):
        with open(path, "w") as f:
            json.dump(self.report, f, indent=2)

    def to_yaml(self, path: str):
        with open(path, "w") as f:
            yaml.safe_dump(self.report, f)
