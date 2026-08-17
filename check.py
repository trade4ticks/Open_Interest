"""
check.py — pre-push gates. Run this before every push.

Exists because a NameError reached main: fetch_chain_intraday.py used
ParquetWriterThread without importing it, and the ad-hoc AST scan that was
supposed to catch it passed clean. That scan compared against a HARDCODED list
of two names it already suspected — it was a targeted search for known-removed
symbols, never a general undefined-name check, so it could not have caught an
undefined name it had not been told about in advance.

py_compile cannot catch it either: an unbound global is a runtime error, and
the module compiles fine. The gate that actually catches this class is
pyflakes, which resolves names against scope.

Gates, run in order, each reported separately:

  1. compile      every .py parses and compiles
  2. names        pyflakes: undefined names are FATAL; other findings are
                  reported but do not fail, so the fatal signal stays legible
  3. imports      every top-level script actually imports. Catches import-time
                  failures that pyflakes cannot see (bad relative import,
                  circular import, a module-level call that raises). Modules
                  whose only problem is a third-party package missing from
                  THIS machine are reported as SKIP, not failure.
  4. tests        every test_*.py, which must exit 0

Usage:
    python check.py            all gates
    python check.py --gate names
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# pyflakes messages that mean the code is broken, as opposed to untidy.
FATAL_PYFLAKES = (
    "undefined name",
    "undefined local",
    "syntax error",
    "redefinition of unused",      # a second def silently shadowing the first
)


def _py_files() -> list[Path]:
    out = [p for p in ROOT.glob("*.py") if p.name != "check.py"]
    out += sorted((ROOT / "lib").glob("*.py"))
    return sorted(out)


def _run(cmd: list[str]) -> tuple[int, str]:
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT,
                       encoding="utf-8", errors="replace")
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def gate_compile() -> bool:
    files = _py_files()
    rc, out = _run([sys.executable, "-m", "py_compile", *map(str, files)])
    if rc != 0:
        print(out.strip())
        print(f"  FAIL — {len(files)} file(s) checked")
        return False
    print(f"  OK — {len(files)} file(s) compile")
    return True


def gate_names() -> bool:
    files = _py_files()
    rc, out = _run([sys.executable, "-m", "pyflakes", *map(str, files)])
    lines = [ln for ln in out.splitlines() if ln.strip()]
    fatal = [ln for ln in lines
             if any(k in ln.lower() for k in FATAL_PYFLAKES)]
    other = [ln for ln in lines if ln not in fatal]
    for ln in fatal:
        print(f"  {ln}")
    if fatal:
        print(f"  FAIL — {len(fatal)} fatal finding(s), "
              f"{len(other)} non-fatal")
        return False
    print(f"  OK — no undefined names in {len(files)} file(s) "
          f"({len(other)} non-fatal finding(s) ignored: unused imports etc.)")
    return True


def gate_imports() -> bool:
    scripts = [p for p in ROOT.glob("*.py")
               if p.name not in ("check.py",) and not p.name.startswith("test_")]
    ok, skipped, failed = 0, [], []
    for p in sorted(scripts):
        rc, out = _run([sys.executable, "-c", f"import {p.stem}"])
        if rc == 0:
            ok += 1
            continue
        # A third-party package absent from this machine is an environment
        # gap, not a defect in the file. A LOCAL module failing to import is.
        missing = ""
        for ln in out.splitlines():
            if "ModuleNotFoundError" in ln and "No module named" in ln:
                missing = ln.split("No module named")[-1].strip().strip("'\"")
        local = missing and (
            (ROOT / f"{missing}.py").exists()
            or (ROOT / missing.replace(".", "/")).with_suffix(".py").exists()
            or (ROOT / missing.split(".")[0]).is_dir())
        if missing and not local:
            skipped.append((p.name, missing))
        else:
            failed.append((p.name, out.strip().splitlines()[-1] if out else "?"))
    for name, err in failed:
        print(f"  {name}: {err}")
    for name, missing in skipped:
        print(f"  SKIP {name} — needs '{missing}', absent on this machine")
    if failed:
        print(f"  FAIL — {len(failed)} script(s) do not import")
        return False
    print(f"  OK — {ok} script(s) import, {len(skipped)} skipped")
    return True


# test_*.py files that are live-endpoint diagnostics rather than unit tests.
# They need a running ThetaData terminal, so they cannot pass on a dev box and
# would make this gate fail permanently — which is how a gate gets ignored.
# Listed explicitly rather than pattern-matched, so adding one is a decision.
NETWORK_TESTS = {
    "test_iv_endpoint.py",      # probes a live terminal; interactive by design
}


def gate_tests() -> bool:
    tests = sorted(ROOT.glob("test_*.py"))
    if not tests:
        print("  OK — no test files")
        return True
    bad = []
    for t in tests:
        if t.name in NETWORK_TESTS:
            print(f"  SKIP {t.name:<29}live-endpoint diagnostic, needs a terminal")
            continue
        rc, out = _run([sys.executable, str(t)])
        lines = out.strip().splitlines()
        # Prefer the test's own verdict line. The last line of combined output
        # is often a logged traceback from a deliberately-exercised failure
        # path, which reads alarmingly next to a passing result.
        verdict = next((ln.strip() for ln in reversed(lines)
                        if "ALL GREEN" in ln or "PASSED" in ln),
                       lines[-1] if lines else "")
        if rc == 0:
            print(f"  {t.name:<34}{verdict}")
        else:
            bad.append(t.name)
            print(f"  {t.name:<34}FAILED")
            print("    " + "\n    ".join(out.strip().splitlines()[-6:]))
    if bad:
        print(f"  FAIL — {len(bad)} test file(s) failed")
        return False
    print(f"  OK — {len(tests) - len(NETWORK_TESTS & {t.name for t in tests})} "
          f"test file(s) passed")
    return True


GATES = {
    "compile": gate_compile,
    "names":   gate_names,
    "imports": gate_imports,
    "tests":   gate_tests,
}


def main() -> int:
    ap = argparse.ArgumentParser(description="Pre-push gates.")
    ap.add_argument("--gate", choices=list(GATES), default=None,
                    help="run one gate instead of all")
    args = ap.parse_args()

    names = [args.gate] if args.gate else list(GATES)
    results = {}
    for i, name in enumerate(names, 1):
        print(f"\n[{i}/{len(names)}] {name}")
        print("-" * 60)
        results[name] = GATES[name]()

    print("\n" + "=" * 60)
    for name, ok in results.items():
        print(f"  {name:<12}{'PASS' if ok else 'FAIL'}")
    print("=" * 60)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
