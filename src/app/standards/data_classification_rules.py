"""
Client-configurable data classification rules.
Each rule explicitly declares its confidence.
"""

DATA_CLASSIFICATION_RULES = {
    # --------------------
    # PII
    # --------------------
    "PII.EMAIL": {
        "patterns": [r"\bemail\b", r"e_mail"],
        "confidence": "HIGH",
    },
    "PII.NAME": {
        "patterns": [r"\bname\b", r"full_name"],
        "confidence": "MEDIUM",
    },
    "PII.DOB": {
        "patterns": [r"\bdob\b", r"date_of_birth", r"birth"],
        "confidence": "HIGH",
    },
    "PII.PHONE": {
        "patterns": [r"phone", r"mobile", r"contact"],
        "confidence": "HIGH",
    },
    "PII.ADDRESS": {
        "patterns": [r"address", r"addr"],
        "confidence": "MEDIUM",
    },
    "PII.NATIONAL_ID": {
        "patterns": [r"aadhaar", r"\bssn\b", r"\bpan\b"],
        "confidence": "HIGH",
    },

    # --------------------
    # SENSITIVE (non-PII)
    # --------------------
    "SENSITIVE.PASSWORD": {
        "patterns": [r"password", r"passwd", r"pwd"],
        "confidence": "HIGH",
    },
    "SENSITIVE.TOKEN": {
        "patterns": [r"token", r"secret", r"api_key"],
        "confidence": "HIGH",
    },
    "SENSITIVE.FINANCIAL": {
        "patterns": [r"salary", r"cost_price", r"revenue"],
        "confidence": "MEDIUM",
    },
    "SENSITIVE.SECURITY": {
        "patterns": [r"ssn_hash", r"encryption_key"],
        "confidence": "HIGH",
    },
}
