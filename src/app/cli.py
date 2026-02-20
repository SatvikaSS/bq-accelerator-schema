import argparse
from email import parser
import json
import os
import shutil
from typing import Any, Dict
from fastapi import Request
from fastapi.responses import Response

from app.router import route

class CLIRequest(Request):
    """
    Minimal Request wrapper for CLI execution.
    Provides headers for identity extraction.
    """

    def __init__(self, user_id: str = "cli_user"):
        scope = {
            "type": "http",
            "headers": [],
        }
        super().__init__(scope)
        self._user_id = user_id

    @property
    def headers(self):
        return {"x-user-id": self._user_id}


def _clean_output_dir(path: str) -> None:
    if not os.path.isdir(path):
        return
    for name in os.listdir(path):
        full = os.path.join(path, name)
        if os.path.isfile(full) or os.path.islink(full):
            os.remove(full)
        elif os.path.isdir(full):
            shutil.rmtree(full)


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _write_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _persist_artifacts(response: Dict[str, Any], output_dir: str) -> None:
    # Always write summary
    summary = {
        "status": response.get("status"),
        "entity": response.get("entity"),
        "version": response.get("version"),
        "decision": response.get("decision"),
        "message": response.get("message"),
        "rename_mappings": response.get("rename_mappings"),
        "partitioning": response.get("partitioning"),
        "clustering": response.get("clustering"),
        "security_summary": response.get("security_summary"),
        "security_analysis": response.get("security_analysis"),
        "schema_drift": response.get("schema_drift"),
        "source_warning": response.get("source_warning"),
        "metadata": response.get("metadata"),
    }
    # Drop null values
    summary = {k: v for k, v in summary.items() if v is not None}

    _write_json(os.path.join(output_dir, "run_summary.json"), summary)

    if "schema_json" in response:
        _write_json(os.path.join(output_dir, "schema.json"), response["schema_json"])

    if "schema_yaml" in response:
        _write_text(os.path.join(output_dir, "schema.yaml"), response["schema_yaml"])

    if "documentation" in response and isinstance(response["documentation"], dict):
        _write_text(
            os.path.join(output_dir, "documentation.md"),
            response["documentation"].get("content", ""),
        )

    if "ddl" in response and isinstance(response["ddl"], dict):
        ddl_text = (
            response["ddl"].get("dataset_ddl", "")
            + "\n\n"
            + response["ddl"].get("table_ddl", "")
        ).strip()
        _write_text(os.path.join(output_dir, "create_table.sql"), ddl_text)

    if "migration_ddls" in response and response["migration_ddls"]:
        migration_text = "\n\n".join(response["migration_ddls"])
        _write_text(os.path.join(output_dir, "migration.sql"), migration_text)


def _persist_response_output(response: Response, output_dir: str, entity: str) -> None:
    body_text = ""
    if getattr(response, "body", None):
        body_text = response.body.decode("utf-8")

    media_type = (getattr(response, "media_type", "") or "").lower()

    if "markdown" in media_type:
        _write_text(os.path.join(output_dir, "documentation.md"), body_text)
        decision = "DOCUMENTATION_ONLY"
        message = "Documentation-only output saved from Response body."
    else:
        _write_text(os.path.join(output_dir, "create_table.sql"), body_text)
        decision = "DDL_ONLY"
        message = "DDL-only output saved from Response body."

    _write_json(
        os.path.join(output_dir, "run_summary.json"),
        {
            "status": "SUCCESS",
            "entity": entity,
            "version": None,
            "decision": decision,
            "message": message,
            "rename_mappings": None,
            "partitioning": None,
            "clustering": None,
            "security_summary": None,
            "security_analysis": None,
            "schema_drift": None,
        },
    )

def _build_payload_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "file_path": args.file,
        "entity": args.entity,
        "domain": args.domain,
        "environment": args.environment,
        "zone": args.zone,
        "layer": args.layer,
        "table_description": args.table_description,
        "output": args.output,
        "drift_policy": args.drift_policy,
        "apply_partitioning": False, # set after suggestion prompt
        "apply_clustering": False, # set after suggestion prompt
        "user_id": args.user_id,
        "return_dict_for_ddl": True,
        "return_dict_for_documentation": True,
    }

def main():
    class C:
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"
        RED = "\033[31m"
        GREEN = "\033[32m"
        YELLOW = "\033[33m"
        BLUE = "\033[34m"
        CYAN = "\033[36m"
        MAGENTA = "\033[35m"

    def cprint(text: str, color: str = C.RESET, bold: bool = False):
        prefix = (C.BOLD if bold else "") + color
        print(f"{prefix}{text}{C.RESET}")

    def _ask_yes_no(prompt: str) -> bool:
        while True:
            cprint(f"{prompt} (yes/no): ", C.YELLOW, bold=True)
            ans = input().strip().lower()
            if ans in {"yes", "y"}:
                return True
            if ans in {"no", "n"}:
                return False
            cprint("Please enter yes or no.", C.RED)

    parser = argparse.ArgumentParser(description="BigQuery Schema Accelerator CLI")

    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--file", help="Source schema file path")
    parser.add_argument("--entity", help="Entity name")
    parser.add_argument("--domain", help="Domain")
    parser.add_argument("--environment", help="Environment")
    parser.add_argument("--zone", help="Zone")
    parser.add_argument("--layer", help="Layer")
    parser.add_argument("--table-description", help="Table description")
    

    parser.add_argument(
        "--output",
        default="ALL_FORMATS",
        choices=["JSON", "YAML", "DDL", "DOCUMENTATION", "ALL_FORMATS"],
        help="Output type",
    )
    parser.add_argument(
        "--drift-policy",
        default="WARN",
        choices=["WARN", "STRICT", "AUTO"],
        help="Drift policy",
    )

    parser.add_argument(
        "--apply-partitioning",
        choices=["yes", "no", "ask"],
        default="ask",
        help="Apply partitioning in DDL: yes/no/ask",
    )
    parser.add_argument(
        "--apply-clustering",
        choices=["yes", "no", "ask"],
        default="ask",
        help="Apply clustering in DDL: yes/no/ask",
    )

    parser.add_argument("--output-dir", default="artifacts")
    parser.add_argument("--clean-output-dir", action="store_true")
    parser.add_argument("--user-id", default="cli_user")

    args = parser.parse_args()

    # Load payload
    if args.config:
        with open(args.config, "r", encoding="utf-8") as f:
            payload = json.load(f)
        payload.setdefault("return_dict_for_ddl", True)
        payload.setdefault("return_dict_for_documentation", True)
        payload.setdefault("apply_partitioning", False)
        payload.setdefault("apply_clustering", False)
    else:
        payload = _build_payload_from_args(args)
        payload["apply_partitioning"] = False
        payload["apply_clustering"] = False

    # Prepare output dir
    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        if args.clean_output_dir:
            _clean_output_dir(args.output_dir)

    request = CLIRequest(user_id=args.user_id)

    try:
        cprint("\n[START] Schema generation started", C.BLUE, bold=True)
        cprint(f"[INFO] Entity={payload.get('entity')}  Output={payload.get('output')}", C.DIM)
        print()

        # Preview call: only suggestions (no full completion logs)
        preview_payload = dict(payload)
        preview_payload["preview_only"] = True
        response = route(preview_payload, request)

        # Malformed CSV confirmation flow
        if isinstance(response, dict) and response.get("decision") == "USER_CONFIRMATION_REQUIRED":
            cprint("\n[WARNING] User confirmation required", C.YELLOW, bold=True)
            cprint(response.get("message", ""), C.YELLOW)
            sw = response.get("source_warning", {})
            for m in sw.get("mismatches", [])[:1]:
                cprint("\n[PREVIEW] Mismatch mapping sample:", C.MAGENTA, bold=True)
                print(json.dumps(m, indent=2))
            if _ask_yes_no("Proceed anyway with malformed CSV"):
                payload.setdefault("csv_override", {})
                payload["csv_override"]["confirm_malformed"] = True
                cprint("[CONFIRM] Proceeding with confirm_malformed=true", C.CYAN)
                print()

                # Re-preview after confirm
                preview_payload = dict(payload)
                preview_payload["preview_only"] = True
                response = route(preview_payload, request)
            else:
                cprint("[STOP] Please fix CSV and re-upload.", C.RED, bold=True)
                return

        # Partitioning / Clustering confirmation flow
        if isinstance(response, dict):
            p_suggestion = response.get("partitioning")
            c_suggestion = response.get("clustering")

            cprint("\n[SUGGESTION] Partitioning", C.MAGENTA, bold=True)
            print(json.dumps(p_suggestion, indent=2))
            print()

            if args.apply_partitioning == "ask":
                payload["apply_partitioning"] = _ask_yes_no("Apply partitioning")
            else:
                payload["apply_partitioning"] = (args.apply_partitioning == "yes")
            print()

            cprint("\n[SUGGESTION] Clustering", C.MAGENTA, bold=True)
            print(json.dumps(c_suggestion, indent=2))
            print()
            
            if args.apply_clustering == "ask":
                payload["apply_clustering"] = _ask_yes_no("Apply clustering")
            else:
                payload["apply_clustering"] = (args.apply_clustering == "yes")
            print()

            cprint(
                f"[CONFIRM] apply_partitioning={payload['apply_partitioning']} "
                f"apply_clustering={payload['apply_clustering']}",
                C.CYAN,
            )
            print()

            # Final run once (this is the actual logged execution)
            payload["preview_only"] = False
            response = route(payload, request)

        # Persist outputs
        if isinstance(response, dict):
            if args.output_dir:
                _persist_artifacts(response, args.output_dir)
                cprint(f"\n[DONE] Artifacts written to: {args.output_dir}", C.GREEN, bold=True)
        else:
            if args.output_dir:
                _persist_response_output(response, args.output_dir, payload.get("entity", "unknown"))
                cprint(f"\n[DONE] Artifacts written to: {args.output_dir}", C.GREEN, bold=True)
            cprint("[INFO] Generated downloadable response output.", C.DIM)

        cprint("[COMPLETE] Schema generation completed", C.GREEN, bold=True)

    except Exception as e:
        cprint("\n[FAILED] Schema generation failed.", C.RED, bold=True)
        cprint(str(e), C.RED)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
