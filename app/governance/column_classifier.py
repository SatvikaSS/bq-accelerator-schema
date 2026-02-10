import re
from typing import Dict

PII_RULES = {
    "PII.EMAIL": [r"\bemail\b", r"e_mail"],
    "PII.NAME": [r"\bname\b", r"full_name"],
    "PII.DOB": [r"\bdob\b", r"date_of_birth", r"birth"],
    "PII.PHONE": [r"phone", r"mobile", r"contact"],
    "PII.ADDRESS": [r"address", r"addr"],
    "PII.NATIONAL_ID": [r"aadhaar", r"ssn", r"pan"],
}

SECURITY_HINTS = {
    "PII.EMAIL": "RESTRICTED_ACCESS",
    "PII.NAME": "RESTRICTED_ACCESS",
    "PII.DOB": "MASK_OR_RESTRICT",
    "PII.PHONE": "RESTRICTED_ACCESS",
    "PII.ADDRESS": "MASK_OR_RESTRICT",
    "PII.NATIONAL_ID": "HIGHLY_RESTRICTED",
}


def classify_column(
    name: str,
    description: str | None = None,
) -> Dict[str, str]:
    """
    Phase-1 PII classifier.
    Rule-based, deterministic, advisory-only.
    """
    text = f"{name} {description or ''}".lower()

    for label, patterns in PII_RULES.items():
        for pattern in patterns:
            if re.search(pattern, text):
                return {
                    "classification": label,
                    "confidence": "HIGH",
                    "recommended_control": SECURITY_HINTS.get(label, "RESTRICTED_ACCESS"),
                }

    return {
        "classification": "NON_PII",
        "confidence": "LOW",
        "recommended_control": "NONE",
    }