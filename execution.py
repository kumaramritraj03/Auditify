"""
Auditify — REPL-Style Code Execution Engine

Design:
  - Breaks generated code into logical chunks (statements / blocks)
  - Executes each chunk sequentially in a subprocess
  - Streams logs in real time via a callback
  - Captures per-step output, detects errors early, stops at failure
  - Maintains shared execution state across steps (persistent namespace)
  - Sandboxed: restricted imports, no os.system/subprocess/eval abuse
  - Returns structured result compatible with the old execute_code() contract

Architecture:
  The subprocess runs a tiny REPL driver that reads chunks from a temp JSON
  manifest, executes them one-by-one inside a shared namespace, and prints
  structured markers to stdout that the parent process parses in real time.

  This keeps execution isolated (separate process) while giving us per-step
  visibility — the best of both worlds.
"""

import subprocess
import json
import os
import sys
import re
import ast
import textwrap
import threading
import queue
import time

# Directory to store generated scripts (next to this file)
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.join(_BASE_DIR, "generated_scripts")
os.makedirs(_SCRIPTS_DIR, exist_ok=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def execute_code(code: str):
    """Legacy-compatible entry point.

    Returns the same dict shape as before:
      {"result": ..., "summary": ..., "error": ..., "logs": ...}

    Internally delegates to the REPL engine.
    """
    print("[FUNCTION] Entering execute_code")
    print(f"[EXECUTION] Code length: {len(code)} chars")
    repl_result = execute_code_repl(code)

    # Map to legacy format
    if repl_result["status"] == "success":
        print("[EXECUTION] Execution completed successfully")
        print("[FUNCTION] Exiting execute_code")
        return {
            "result": repl_result["result"],
            "summary": "Execution successful",
            "error": None,
            "logs": "\n".join(repl_result["logs"]),
        }
    else:
        print(f"[EXECUTION] [ERROR] Execution failed: {str(repl_result.get('error', ''))[:200]}")
        print("[FUNCTION] Exiting execute_code")
        return {
            "result": None,
            "summary": "Execution failed",
            "error": repl_result["error"],
            "logs": "\n".join(repl_result["logs"]),
        }


def execute_code_repl(code: str, on_step=None, timeout=120):
    """REPL-style execution engine.

    Args:
        code:     Raw LLM-generated code (may include markdown fences).
        on_step:  Optional callback called after each chunk executes.
                  Signature: on_step(step_info: dict) where step_info has:
                    {"step": int, "total": int, "label": str,
                     "status": "running"|"success"|"error",
                     "output": str, "error": str|None}
        timeout:  Max seconds for the entire execution.

    Returns:
        {
          "status": "success" | "error",
          "logs": [str, ...],          # all captured output lines
          "steps": [step_info, ...],   # per-chunk detail
          "result": <final value>,     # parsed from result variable
          "error": str | None,
        }
    """
    print("[FUNCTION] Entering execute_code_repl")
    clean_code = _strip_code_fences(code)

    # 1. Validate safety
    print("[EXECUTION] Step 1: Validating code safety...")
    safety_error = _validate_code_safety(clean_code)
    if safety_error:
        err = f"Code safety violation: {safety_error}"
        print(f"[EXECUTION] [ERROR] Safety violation: {safety_error}")
        if on_step:
            on_step({"step": 0, "total": 0, "label": "Safety Check",
                      "status": "error", "output": "", "error": err})
        return {"status": "error", "logs": [], "steps": [], "result": None, "error": err}

    print("[EXECUTION] Safety check passed")

    # 2. Chunk the code
    print("[EXECUTION] Step 2: Chunking code...")
    chunks = _chunk_code(clean_code)
    if not chunks:
        err = "No executable code found."
        print(f"[EXECUTION] [ERROR] {err}")
        return {"status": "error", "logs": [], "steps": [], "result": None, "error": err}

    print(f"[EXECUTION] Code chunked into {len(chunks)} chunks")
    for i, c in enumerate(chunks):
        print(f"[EXECUTION]   Chunk {i}: {c['label']}")

    # 3. Build the REPL driver script and manifest
    manifest_path = os.path.join(_SCRIPTS_DIR, "repl_manifest.json")
    driver_path = os.path.join(_SCRIPTS_DIR, "repl_driver.py")

    manifest = {
        "chunks": [{"index": i, "code": c["code"], "label": c["label"]} for i, c in enumerate(chunks)],
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False)

    _write_repl_driver(driver_path, manifest_path)

    # 4. Execute with real-time streaming
    print("[EXECUTION] Step 3: Launching subprocess for REPL execution...")
    all_logs = []
    steps = []
    result_value = None
    error_value = None
    current_step = {"step": 0, "total": len(chunks), "label": "", "status": "running", "output": "", "error": None}

    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", driver_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=_BASE_DIR,
            bufsize=1,
        )
    except Exception as e:
        err = f"Failed to start execution process: {e}"
        print(f"[EXECUTION] [ERROR] {err}")
        return {"status": "error", "logs": [], "steps": [], "result": None, "error": err}

    # Read stderr in background thread to avoid deadlock
    stderr_lines = []
    stderr_q = queue.Queue()

    def _read_stderr():
        for line in proc.stderr:
            stderr_q.put(line.rstrip("\n"))
        stderr_q.put(None)  # sentinel

    stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
    stderr_thread.start()

    start_time = time.time()
    timed_out = False

    try:
        for raw_line in proc.stdout:
            # Check timeout
            if time.time() - start_time > timeout:
                proc.kill()
                timed_out = True
                break

            line = raw_line.rstrip("\n")

            # Parse structured markers from the REPL driver
            if line.startswith("__STEP_START__:"):
                payload = _safe_json(line[len("__STEP_START__:"):])
                if payload:
                    current_step = {
                        "step": payload.get("index", 0),
                        "total": len(chunks),
                        "label": payload.get("label", ""),
                        "status": "running",
                        "output": "",
                        "error": None,
                    }
                    if on_step:
                        on_step(current_step)

            elif line.startswith("__STEP_END__:"):
                payload = _safe_json(line[len("__STEP_END__:"):])
                if payload:
                    current_step["status"] = payload.get("status", "success")
                    current_step["output"] = payload.get("output", "")
                    if payload.get("error"):
                        current_step["error"] = payload["error"]
                        current_step["status"] = "error"
                    steps.append(dict(current_step))
                    if on_step:
                        on_step(current_step)
                    # If step failed, the driver will stop — keep reading for final markers
                    if current_step["status"] == "error":
                        error_value = current_step["error"]

            elif line.startswith("__AUDITIFY_RESULT__:"):
                json_str = line[len("__AUDITIFY_RESULT__:"):]
                result_value = _safe_json(json_str)

            elif line.startswith("__LOG__:"):
                log_line = line[len("__LOG__:"):]
                all_logs.append(log_line)

            else:
                # Plain print output from user code
                all_logs.append(line)

    except Exception as e:
        error_value = error_value or f"Error reading execution output: {e}"

    # Wait for process to finish
    try:
        proc.wait(timeout=max(1, timeout - (time.time() - start_time)))
    except subprocess.TimeoutExpired:
        proc.kill()
        timed_out = True

    # Drain stderr
    while True:
        try:
            item = stderr_q.get(timeout=1)
            if item is None:
                break
            stderr_lines.append(item)
        except queue.Empty:
            break

    if timed_out:
        error_value = f"Code execution timed out after {timeout} seconds."

    # If process failed but we didn't catch a step error
    if proc.returncode and proc.returncode != 0 and not error_value:
        error_value = "\n".join(stderr_lines) if stderr_lines else "Unknown execution error"

    # If no result was produced
    if error_value is None and result_value is None:
        error_value = "Code did not produce a 'result' variable."

    status = "success" if error_value is None else "error"

    print(f"[EXECUTION] REPL execution finished | status={status} | steps={len(steps)} | logs={len(all_logs)}")
    if error_value:
        print(f"[EXECUTION] [ERROR] {str(error_value)[:200]}")
    print("[FUNCTION] Exiting execute_code_repl")

    return {
        "status": status,
        "logs": all_logs,
        "steps": steps,
        "result": result_value,
        "error": error_value,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CODE CHUNKING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _chunk_code(code: str):
    """Break code into logical executable chunks using AST parsing.

    Each chunk is one or more top-level statements that form a logical unit.
    We group: imports together, assignments together with their immediate
    dependents, and keep compound statements (if/for/try/with/def/class)
    as single chunks.

    Falls back to line-by-line if AST parsing fails.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        # Fallback: treat entire code as one chunk
        return [{"code": code, "label": "Execute code"}]

    if not tree.body:
        return []

    raw_stmts = []
    for node in tree.body:
        start = node.lineno - 1  # 0-indexed
        end = node.end_lineno     # 1-indexed, exclusive after slicing
        raw_stmts.append({
            "node": node,
            "start": start,
            "end": end,
        })

    lines = code.splitlines()
    chunks = []
    group = []
    group_type = None  # "import", "assign", None

    def flush_group():
        nonlocal group, group_type
        if not group:
            return
        start = group[0]["start"]
        end = group[-1]["end"]
        chunk_code = "\n".join(lines[start:end])
        label = _label_for_group(group, group_type)
        chunks.append({"code": chunk_code, "label": label})
        group = []
        group_type = None

    for stmt in raw_stmts:
        node = stmt["node"]
        ntype = _classify_node(node)

        if ntype == "import":
            if group_type == "import":
                group.append(stmt)
            else:
                flush_group()
                group_type = "import"
                group.append(stmt)
        elif ntype == "assign":
            if group_type == "assign":
                group.append(stmt)
            else:
                flush_group()
                group_type = "assign"
                group.append(stmt)
        else:
            # Compound or expression — flush previous group, emit as own chunk
            flush_group()
            chunk_code = "\n".join(lines[stmt["start"]:stmt["end"]])
            label = _label_for_node(node)
            chunks.append({"code": chunk_code, "label": label})

    flush_group()
    return chunks


def _classify_node(node):
    """Classify AST node into import/assign/compound."""
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return "import"
    if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
        return "assign"
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
        # Standalone function calls like print(...) — treat as compound
        return "compound"
    if isinstance(node, ast.Expr):
        return "assign"  # simple expressions, group them
    return "compound"


def _label_for_group(group, group_type):
    """Generate a human-readable label for a group of statements."""
    if group_type == "import":
        modules = []
        for stmt in group:
            node = stmt["node"]
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                modules.append(node.module or "")
        return f"Import {', '.join(m for m in modules[:3] if m)}" + ("..." if len(modules) > 3 else "")
    elif group_type == "assign":
        targets = []
        for stmt in group:
            node = stmt["node"]
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        targets.append(t.id)
            elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
                targets.append(node.target.id)
        if targets:
            return f"Set up {', '.join(targets[:3])}" + ("..." if len(targets) > 3 else "")
        return "Variable assignments"
    return "Execute block"


def _label_for_node(node):
    """Generate a human-readable label for a single AST node."""
    if isinstance(node, (ast.For, ast.AsyncFor)):
        return "Loop execution"
    if isinstance(node, (ast.While,)):
        return "While loop"
    if isinstance(node, (ast.If,)):
        return "Conditional block"
    if isinstance(node, (ast.With, ast.AsyncWith)):
        return "Context manager block"
    if isinstance(node, (ast.Try,)):
        return "Try/except block"
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return f"Define function {node.name}()"
    if isinstance(node, ast.ClassDef):
        return f"Define class {node.name}"
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
        func = node.value.func
        if isinstance(func, ast.Name):
            return f"Call {func.id}()"
        elif isinstance(func, ast.Attribute):
            return f"Call .{func.attr}()"
    return "Execute statement"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SAFETY VALIDATION (SANDBOX)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# Imports that are allowed in generated code
ALLOWED_IMPORTS = frozenset({
    "pandas", "pd", "numpy", "np", "duckdb", "json", "os", "datetime",
    "math", "re", "collections", "itertools", "functools", "decimal",
    "csv", "io", "pathlib", "typing", "warnings", "statistics",
    "openpyxl", "xlrd", "dateutil",
})

# Dangerous patterns (function calls / attribute access)
_DANGEROUS_PATTERNS = [
    r'\bos\.system\s*\(',
    r'\bos\.popen\s*\(',
    r'\bos\.exec\w*\s*\(',
    r'\bos\.spawn\w*\s*\(',
    r'\bos\.remove\s*\(',
    r'\bos\.rmdir\s*\(',
    r'\bos\.unlink\s*\(',
    r'\bshutil\.rmtree\s*\(',
    r'\bsubprocess\.',
    r'\b__import__\s*\(',
    r'\beval\s*\(',
    r'\bexec\s*\(',
    r'\bcompile\s*\(',
    r'\bglobals\s*\(\s*\)',
    r'\blocals\s*\(\s*\)',
    r'\bgetattr\s*\(.+,\s*["\']__',
    r'\bsetattr\s*\(',
    r'\bdelattr\s*\(',
    r'\bopen\s*\(.*(["\']\s*w|["\']\s*a)',   # open() in write/append mode
    r'\bsocket\.',
    r'\brequests\.',
    r'\burllib\.',
    r'\bhttp\.',
    r'\bsmtplib\.',
    r'\bctypes\.',
    r'\bsys\.exit\s*\(',
    r'\bquit\s*\(',
    r'\bexit\s*\(',
]


def _validate_code_safety(code: str):
    """Check code for dangerous patterns. Returns error string or None if safe."""
    # Check imports via AST
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"Syntax error in generated code: {e}"

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_module = alias.name.split(".")[0]
                if root_module not in ALLOWED_IMPORTS:
                    return f"Import of '{alias.name}' is not allowed. Allowed: {sorted(ALLOWED_IMPORTS)}"
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                root_module = node.module.split(".")[0]
                if root_module not in ALLOWED_IMPORTS:
                    return f"Import from '{node.module}' is not allowed. Allowed: {sorted(ALLOWED_IMPORTS)}"

    # Check dangerous patterns via regex
    for pattern in _DANGEROUS_PATTERNS:
        match = re.search(pattern, code)
        if match:
            return f"Dangerous operation detected: '{match.group(0).strip()}'"

    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REPL DRIVER (written to disk, executed as subprocess)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _write_repl_driver(driver_path: str, manifest_path: str):
    """Write the subprocess REPL driver script.

    This script:
      1. Reads the chunk manifest
      2. Executes each chunk in a shared namespace
      3. Prints structured markers for the parent to parse
      4. Captures per-step stdout
      5. Serializes the final `result` variable
    """
    # Use raw strings and escape braces for the template
    driver_code = textwrap.dedent('''\
        import sys
        import json
        import io
        import traceback
        import datetime

        # Standard imports available to user code
        import pandas as pd
        import duckdb
        import os

        # Shared namespace for all chunks
        _ns = {
            "pd": pd, "pandas": pd,
            "duckdb": duckdb,
            "os": os,
            "json": json,
            "datetime": datetime,
            "__builtins__": __builtins__,
        }

        # Try optional imports
        try:
            import numpy as np
            _ns["np"] = np
            _ns["numpy"] = np
        except ImportError:
            pass

        def _serialize(obj):
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient='records')
            elif isinstance(obj, pd.Series):
                return obj.to_dict()
            elif hasattr(obj, 'fetchdf'):
                return obj.fetchdf().to_dict(orient='records')
            elif hasattr(obj, 'fetchall'):
                return [list(row) for row in obj.fetchall()]
            return obj

        def _json_default(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            if isinstance(o, float) and (o != o):
                return None
            try:
                return str(o)
            except Exception:
                return None

        # Load manifest
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        chunks = manifest["chunks"]

        for chunk in chunks:
            idx = chunk["index"]
            code = chunk["code"]
            label = chunk["label"]

            # Signal step start
            print("__STEP_START__:" + json.dumps({"index": idx, "label": label}), flush=True)

            # Capture stdout for this step
            old_stdout = sys.stdout
            capture = io.StringIO()
            sys.stdout = capture

            step_error = None
            try:
                exec(code, _ns)
            except Exception:
                step_error = traceback.format_exc()

            sys.stdout = old_stdout
            captured = capture.getvalue()

            # Print captured output as log lines
            if captured:
                for ln in captured.splitlines():
                    print("__LOG__:" + ln, flush=True)

            # Signal step end
            step_result = {
                "index": idx,
                "status": "error" if step_error else "success",
                "output": captured.strip(),
                "error": step_error,
            }
            print("__STEP_END__:" + json.dumps(step_result), flush=True)

            if step_error:
                # Stop execution on error
                sys.exit(1)

        # After all chunks, serialize result
        _result = _ns.get("result", None)
        if _result is not None:
            serialized = _serialize(_result)
            print("__AUDITIFY_RESULT__:" + json.dumps(serialized, default=_json_default), flush=True)
        else:
            # No result variable — signal that
            print("__AUDITIFY_RESULT__:" + "null", flush=True)
    ''').replace("MANIFEST_PATH", repr(manifest_path.replace("\\", "/")))

    with open(driver_path, "w", encoding="utf-8") as f:
        f.write(driver_code)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# UTILITIES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _strip_code_fences(code: str) -> str:
    """Remove markdown code fences that LLMs often wrap code in."""
    clean = code.strip()
    if clean.startswith("```python"):
        clean = clean[len("```python"):]
    elif clean.startswith("```"):
        clean = clean[3:]
    if clean.endswith("```"):
        clean = clean[:-3]
    return clean.strip()


def _safe_json(s: str):
    """Parse JSON string, return None on failure."""
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return None
