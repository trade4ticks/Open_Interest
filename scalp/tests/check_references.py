"""Resolve every cross-module attribute reference in scalp/ without importing.

    python -m scalp.tests.check_references

WHY THIS EXISTS. Three bugs have now reached the VPS the same way: code that
was written, byte-compiled, pushed, and never executed. `td.today_et()` was
called from update_universe and never defined. `profile_compute` called
`metrics.condition_code_sets` after it had been deleted. Both are one-line
mistakes that `py_compile` cannot see, because it checks syntax and stops.

WHY AST AND NOT A LINTER. pyflakes and ruff are single-module: they flag
undefined LOCAL names, but neither resolves `td.today_et` against the contents
of `scalp/thetadata.py`, so neither would have caught any of these. A type
checker (mypy, pyright) would — but the checking environment has no pandas,
numpy, psycopg2 or pyarrow installed, so anything that imports the package
cannot run at all. This parses instead of importing, needs nothing but the
standard library, and therefore runs anywhere including a bare interpreter.

WHAT IT CATCHES
    module.name references where `name` is not defined at that module's top
    level — misspellings, deletions, and functions that were called before
    they were written.

WHAT IT DOES NOT CATCH
    Wrong argument counts or types, attributes on instances, anything created
    dynamically, and any error that only appears when the code runs. It is a
    floor, not a substitute for the smoke test.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
PACKAGE = PACKAGE_ROOT.name


def module_files() -> dict[str, Path]:
    """Dotted module name -> file, for every .py under the package."""
    out: dict[str, Path] = {}
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PACKAGE_ROOT).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        out[".".join([PACKAGE] + parts)] = path
    return out


def _bound_names(body: list[ast.stmt]) -> set[str]:
    """Names bound at this block's level, without descending into functions.

    Recurses through if/try/for/while/with so a conditionally defined
    module-level name still counts as defined — a definition inside `try:` is
    still a definition.
    """
    names: set[str] = set()
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                names.update(_target_names(target))
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            names.update(_target_names(node.target))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name)
        elif isinstance(node, (ast.If, ast.Try, ast.For, ast.While, ast.With)):
            names |= _bound_names(node.body)
            names |= _bound_names(getattr(node, "orelse", []) or [])
            names |= _bound_names(getattr(node, "finalbody", []) or [])
            for handler in getattr(node, "handlers", []) or []:
                names |= _bound_names(handler.body)
    return names


def _target_names(target: ast.expr) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        out: set[str] = set()
        for element in target.elts:
            out |= _target_names(element)
        return out
    return set()


def module_aliases(tree: ast.Module, known: set[str]) -> dict[str, str]:
    """Local name -> dotted module, for imports of modules inside this package."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in known:
                    aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom):
            if not node.module:
                continue
            for alias in node.names:
                candidate = f"{node.module}.{alias.name}"
                if candidate in known:
                    aliases[alias.asname or alias.name] = candidate
    return aliases


def check() -> list[str]:
    files = module_files()
    trees: dict[str, ast.Module] = {}
    exports: dict[str, set[str]] = {}

    for name, path in files.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        trees[name] = tree
        exports[name] = _bound_names(tree.body)

    known = set(files)
    problems: list[str] = []

    for name, tree in trees.items():
        aliases = module_aliases(tree, known)
        if not aliases:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if not isinstance(node.value, ast.Name):
                continue
            target = aliases.get(node.value.id)
            if target is None:
                continue
            if node.attr in exports[target]:
                continue
            # Dunders resolve on the module object itself.
            if node.attr.startswith("__") and node.attr.endswith("__"):
                continue
            rel = files[name].relative_to(PACKAGE_ROOT.parent)
            problems.append(
                f"{rel}:{node.lineno}: {node.value.id}.{node.attr} "
                f"is not defined in {target}"
            )
    return sorted(set(problems))


def main() -> None:
    problems = check()
    files = module_files()
    print(f"checked {len(files)} modules in {PACKAGE}/")
    if not problems:
        print("no unresolved cross-module references")
        return
    print()
    for problem in problems:
        print(f"  {problem}")
    print()
    print(f"{len(problems)} unresolved reference(s)")
    sys.exit(1)


if __name__ == "__main__":
    main()
