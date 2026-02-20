import logging
import json
import sys
import time
import uuid
import os

class _C:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"


def _use_color() -> bool:
    # Color only if explicitly enabled and terminal supports it.
    return os.getenv("LOG_COLOR", "0") == "1" and sys.stdout.isatty()

def _event_color(event_type: str) -> str:
    et = (event_type or "").upper()
    if "FAILED" in et or "ERROR" in et:
        return _C.RED
    if "WARNING" in et:
        return _C.YELLOW
    if "STARTED" in et:
        return _C.GREEN
    if "COMPLETED" in et:
        return _C.GREEN
    if "AUDIT" in et:
        return _C.CYAN
    return _C.MAGENTA

def get_logger():
    logger = logging.getLogger("schema_accelerator")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

logger = get_logger()

# Request ID Generator
def generate_request_id():
    return str(uuid.uuid4())

# Structured Log Event
def log_event(event_type: str, payload: dict):
    record = {"event_type": event_type, **payload}
    text = json.dumps(record)

    if _use_color():
        color = _event_color(event_type)
        logger.info(f"{color}{text}{_C.RESET}")
    else:
        logger.info(text)

    logger.info("") 

# Timer Utility
class RequestTimer:
    """
    Simple execution timer.
    """
    def __init__(self):
        self.start_time = time.time()

    def duration(self):
        return round(time.time() - self.start_time, 4)