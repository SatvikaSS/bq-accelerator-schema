import json
import os
import sys
import uuid
from datetime import datetime
from typing import Dict

class _C:
    RESET = "\033[0m"
    CYAN = "\033[36m"

def _use_color() -> bool:
    return os.getenv("LOG_COLOR", "0") == "1" and sys.stdout.isatty()

class AuditLogger:
    """
    Responsible for building and persisting audit records.
    """
    def build_record(
        self,
        request_id: str,
        user_id: str,
        action: str,
        entity: str,
        version: str,
        decision: str,
        breaking_changes: int,
        non_breaking_changes: int,
        security_summary: Dict,
    ) -> Dict:
        return {
            "audit_id": str(uuid.uuid4()),
            "request_id": request_id,
            "user_id": user_id,
            "action": action,
            "entity": entity,
            "version": version,
            "decision": decision,
            "breaking_changes": breaking_changes,
            "non_breaking_changes": non_breaking_changes,
            "pii_detected": security_summary.get("pii_detected", False),
            "sensitive_detected": security_summary.get("sensitive_detected", False),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def persist(self, record: Dict):
        """
        For now: structured log output.
        In Cloud Run this goes to Cloud Logging.
        """
        text = json.dumps({"AUDIT_EVENT": record})
        if _use_color():
            print(f"{_C.CYAN}{text}{_C.RESET}")
        else:
            print(text)
        print() 
