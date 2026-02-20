import os
import csv
from typing import List, Dict, Optional
from app.canonical.field import CanonicalField
from app.canonical.table import CanonicalTable
from app.canonical.schema import CanonicalSchema
from app.inference.type_inference import infer_type, is_ambiguous_boolean
from app.inference.numeric_inference import infer_numeric_metadata
from app.pipeline.naming import normalize_identifier

ALLOWED_CANONICAL_TYPES = {
    "STRING",
    "INTEGER",
    "FLOAT",
    "DECIMAL",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "JSON",
    "RECORD",
}
# ------------------------------------------------------------------
# Delimiter detection
# ------------------------------------------------------------------
def detect_delimiter_from_lines(lines: List[str]) -> str:
    """
    Robust delimiter detection with safe fallback.
    """
    sample = "\n".join(lines[:20])
    try:
        dialect = csv.Sniffer().sniff(
            sample,
            delimiters=[",", ";", "\t", "|"]
        )
        return dialect.delimiter
    except csv.Error:
        if ";" in sample:
            return ";"
        elif "," in sample:
            return ","
        elif "\t" in sample:
            return "\t"
        elif "|" in sample:
            return "|"
        else:
            return ","

# ------------------------------------------------------------------
# CSV Adapter
# ------------------------------------------------------------------
class CSVAdapter:
    """
    CSV ingestion adapter.
    Responsibilities:
    - Remove comments and empty lines
    - Detect delimiter
    - Read header safely
    - Repair empty column names using entity_name + position
    - Sample rows
    - Infer column names, data types, nullability, and lengths
    - Produce CanonicalSchema
    DOES NOT:
    - Normalize names
    - Deduplicate column names
    - Apply platform-specific rules
    """
    def __init__(
        self,
        file_path: str,
        entity_name: Optional[str] = None,
        sample_size: int = 100,
        override: Optional[Dict] = None,
        type_overrides: Optional[Dict[str, str]] = None,
    ):
        self.file_path = file_path
        self.sample_size = sample_size
        self.override = override or {}
        self.header_mode = str(self.override.get("header_mode", "AUTO")).upper()
        if self.header_mode not in {"AUTO", "PRESENT", "ABSENT"}:
            raise ValueError("csv_override.header_mode must be one of: AUTO, PRESENT, ABSENT")
        self.type_overrides = type_overrides or {}

        for key, value in self.type_overrides.items():
            normalized = str(value).upper()
            if normalized not in ALLOWED_CANONICAL_TYPES:
                raise ValueError(
                    f"Invalid type override for column '{key}': '{value}'. "
                    f"Allowed: {sorted(ALLOWED_CANONICAL_TYPES)}"
                )
        if entity_name:
            self.entity_name = entity_name
        else:
            self.entity_name = os.path.splitext(
                os.path.basename(file_path)
            )[0]
    
    def _compute_stats(self, values: List[str]) -> Dict[str, float]:
        total = len(values)
        if total == 0:
            return {}
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        null_count = total - len(non_null)
        distinct = len(set(str(v).strip() for v in non_null)) if non_null else 0
        return {
            "distinct_ratio": round(distinct / total, 4),
            "null_ratio": round(null_count / total, 4),
        }
    def _build_row_mismatch_entry(self, row_num: int, header: List[str], row: List[str]) -> Dict:
        mapped = {}
        for idx, col in enumerate(header):
            mapped[col] = row[idx] if idx < len(row) else "<MISSING>"

        extra_values = row[len(header):] if len(row) > len(header) else []
        missing_columns = header[len(row):] if len(row) < len(header) else []

        return {
            "row_number": row_num,  # 1-based file line number
            "expected_columns": len(header),
            "actual_columns": len(row),
            "mapped_preview": mapped,
            "extra_values": extra_values,
            "missing_columns": missing_columns,
        }
    # --------------------------------------------------
    # REQUIRED BY ROUTER
    # --------------------------------------------------
    def parse(self) -> CanonicalSchema:
        """
        Router entrypoint.
        """
        return self._parse_csv()
    
    def _detect_header(self, clean_lines: List[str], delimiter: str) -> bool:
        if self.header_mode == "PRESENT":
            return True
        if self.header_mode == "ABSENT":
            return False

        sample = "\n".join(clean_lines[:20])
        try:
            return csv.Sniffer().has_header(sample)
        except csv.Error:
            return True  # safe default
    
    def _validate_no_mixed_delimiters(self, raw_header: List[str], selected_delimiter: str) -> None:
        other_delims = [",", ";", "\t", "|"]
        if selected_delimiter in other_delims:
            other_delims.remove(selected_delimiter)

        suspicious = []
        for h in raw_header:
            token = (h or "").strip()
            if any(d in token for d in other_delims):
                suspicious.append(token)

        if suspicious:
            raise ValueError(
                f"Mixed delimiters detected in header. Selected delimiter '{selected_delimiter}', "
                f"but header tokens also contain other delimiters. Offending header tokens: {suspicious}"
            )

    def _validate_quote_balance(self, lines: List[str]) -> None:
        for i, line in enumerate(lines, start=1):
            s = line.rstrip("\r\n")
            # count unescaped double quotes
            quote_count = 0
            j = 0
            while j < len(s):
                if s[j] == '"':
                    if j + 1 < len(s) and s[j + 1] == '"':  # escaped quote
                        j += 2
                        continue
                    quote_count += 1
                j += 1

            if quote_count % 2 != 0:
                raise ValueError(f"Malformed CSV: unbalanced quotes at line {i}")

    def _looks_like_header_row(self, first_row: List[str]) -> bool:
        if not first_row:
            return False
        non_empty = [c.strip() for c in first_row if c and c.strip()]
        if not non_empty:
            return False
        # header-like if most tokens are identifier-ish and not pure numbers
        identifierish = 0
        for c in non_empty:
            if not c.replace("_", "").replace("-", "").replace(" ", "").isalnum():
                continue
            if c.replace(".", "", 1).isdigit():
                continue
            identifierish += 1
        return identifierish / len(non_empty) >= 0.6
    
    # --------------------------------------------------
    # Core CSV parsing logic
    # --------------------------------------------------
    def _parse_csv(self) -> CanonicalSchema:
        read_limit = self.sample_size + 50
        clean_lines = self._read_clean_lines(self.file_path, limit=read_limit)

        if not clean_lines:
            raise ValueError("CSV contains no valid (non-comment) lines")
        
        self._validate_quote_balance(clean_lines)

        # Optional quote removal for malformed exports where full lines are wrapped
        if self.override and self.override.get("remove_quotes"):
            processed = []
            for line in clean_lines:
                stripped = line.strip()
                if stripped.startswith('"') and stripped.endswith('"'):
                    stripped = stripped[1:-1].replace('""', '"')
                processed.append(stripped)
            clean_lines = processed
        # Re-validate after quote normalization
        self._validate_quote_balance(clean_lines)

        # Delimiter selection
        if self.override and self.override.get("mode") == "SPLIT":
            delimiter = self.override.get("delimiter")
            if not delimiter:
                raise ValueError("Override mode 'SPLIT' requires a non-empty delimiter")
            if delimiter == "\\t":
                delimiter = "\t"
        elif self.override and self.override.get("mode") == "SINGLE":
            delimiter = "\u0000"
        else:
            delimiter = detect_delimiter_from_lines(clean_lines)

        has_header = self._detect_header(clean_lines, delimiter)
        preview_reader = csv.reader(clean_lines[:2], delimiter=delimiter)
        first_row = next(preview_reader, [])
        if not has_header and self._looks_like_header_row(first_row):
            has_header = True

        if not has_header:
            raise ValueError(
            "CSV header row not detected. First row appears to be data. "
            "Add a header row or set csv_override.header_mode='PRESENT' if header exists."
        )

        reader = csv.reader(clean_lines, delimiter=delimiter)
        raw_header = next(reader, None)

        if not raw_header or not any(h.strip() for h in raw_header):
            raise ValueError("CSV has no headers after removing comments")

        self._validate_no_mixed_delimiters(raw_header, delimiter)
        
        # Malformed detection for guided retry
        malformed_warning = None
        if len(raw_header) == 1 and not self.override:
            header_value = raw_header[0].strip()
            if any(d in header_value for d in [",", ";", "|", "\t", " "]):
                malformed_warning = {
                    "type": "POTENTIAL_MALFORMED_CSV",
                    "message": (
                        "CSV header contains embedded delimiters. "
                        "User confirmation required."
                    ),
                    "detected_header": header_value,
                }

        # Build safe header
        header: List[str] = []
        for idx, h in enumerate(raw_header, start=1):
            if h and h.strip():
                header.append(h.strip())
            else:
                header.append(f"{self.entity_name}_{idx}")

        confirm_malformed = bool(self.override.get("confirm_malformed", False))
        row_mismatches: List[Dict] = []
        MAX_MISMATCH_PREVIEW = 1
        # Sample rows
        column_samples: Dict[str, List[str]] = {col: [] for col in header}
        total_rows = 0
        for i, row in enumerate(reader):
            total_rows += 1
            row_num = i + 2  # header is row 1
            if len(row) != len(header):
                if len(row_mismatches) < MAX_MISMATCH_PREVIEW:
                    row_mismatches.append(
                        self._build_row_mismatch_entry(row_num=row_num, header=header, row=row)
                    )          
            if i >= self.sample_size:
                break
            for idx, col in enumerate(header):
                value = row[idx] if idx < len(row) else ""
                column_samples[col].append(value)

        forced_missing_cols = set()
        for m in row_mismatches:
            for c in m.get("missing_columns", []):
                forced_missing_cols.add(c)

        fields = self._infer_fields(
            header,
            column_samples,
            forced_missing_cols=forced_missing_cols,
            confirm_malformed=confirm_malformed,
        )

        table = CanonicalTable(
            name=self.entity_name,
            fields=fields,
            metadata={
                "row_count": total_rows,
                "row_count_mode": "sampled",
                "row_count_sample_size": self.sample_size,
            },
        )

        metadata = {
            "sample_size": self.sample_size,
            "delimiter": delimiter,
            "source_file": self.file_path,
            "comments_ignored": True,
        }
        if malformed_warning:
            metadata["source_warning"] = malformed_warning

        if row_mismatches:
            metadata["source_warning"] = {
                "type": "ROW_WIDTH_MISMATCH",
                "message": (
                    "Some rows have different column counts than the header. "
                    "Review mapping preview and confirm to continue."
                ),
                "header_columns": len(header),
                "rows_examined": total_rows,
                "mismatch_count_in_preview": len(row_mismatches),
                "mismatches": row_mismatches,
                "confirm_required": not confirm_malformed,
                "suggested_correction": (
                    "Ensure each data row has the same number of delimiter-separated values as the header, "
                    "or re-upload corrected CSV."
                ),
            }

        return CanonicalSchema(
            source_type="csv",
            dataset={
                "domain": None,
                "environment": None,
                "zone": None,
                "layer": None,
                "entity": self.entity_name,
            },
            tables=[table],
            metadata=metadata,
        )
    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------
    def _read_clean_lines(self, file_path: str, limit: Optional[int] = None) -> List[str]:
        """
        Removes:
        - empty lines
        - comment lines starting with '#' or '--'

        If limit is provided, stops after collecting `limit` valid lines.
        """
        valid_lines: List[str] = []
        count = 0

        with open(file_path, encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                if limit is not None and count >= limit:
                    break

                if line.strip() and not line.lstrip().startswith(("#", "--")):
                    valid_lines.append(line)
                    count += 1

        return valid_lines

    def _infer_fields(
        self,
        header: List[str],
        column_samples: Dict[str, List[str]],
        forced_missing_cols: Optional[set] = None,
        confirm_malformed: bool = False,
    ) -> List[CanonicalField]:
        forced_missing_cols = forced_missing_cols or set()
        fields: List[CanonicalField] = []

        for col in header:
            values = column_samples[col]
            data_type = infer_type(values)
            ambiguous = (data_type == "BOOLEAN" and is_ambiguous_boolean(values))

            norm_col = normalize_identifier(col)
            if norm_col in self.type_overrides:
                data_type = self.type_overrides[norm_col].upper()
            elif col in self.type_overrides:
                data_type = self.type_overrides[col].upper()

            nullable = any((v is None) or (str(v).strip() == "") for v in values)

            max_length = None
            if data_type == "STRING" and values:
                non_empty_values = [str(v) for v in values if v is not None and str(v).strip() != ""]
                if non_empty_values:
                    max_length = max(len(v) for v in non_empty_values)

            numeric_metadata = None
            if data_type == "DECIMAL":
                numeric_metadata = infer_numeric_metadata(values)

            if confirm_malformed and col in forced_missing_cols:
                data_type = "STRING"
                nullable = False
                max_length = None
                numeric_metadata = None

            fields.append(
                CanonicalField(
                    name=col,
                    data_type=data_type,
                    nullable=nullable,
                    has_missing=nullable,
                    max_length=max_length,
                    numeric_metadata=numeric_metadata,
                    is_ambiguous_boolean=ambiguous,
                    stats=self._compute_stats(values),
                )
            )

        return fields

