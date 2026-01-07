#!/usr/bin/env python3
from __future__ import annotations

import ast
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class AssetFn:
    file: Path  # path relative to repo root
    name: str  # function name
    code_version: Optional[str]
    body_fingerprint: str  # stable string for “function changed?”


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).rstrip("\n")


def _git_maybe(*args: str) -> Optional[str]:
    try:
        return subprocess.check_output(
            ["git", *args],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return None


def _python_files_in_repo() -> Iterable[Path]:
    # Avoid scanning venvs / build dirs; tweak if you use different layouts
    skip_dirs = {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        "build",
        "dist",
        ".mypy_cache",
        ".ruff_cache",
    }
    for p in REPO_ROOT.rglob("*.py"):
        if any(part in skip_dirs for part in p.parts):
            continue
        yield p


def _is_asset_decorator(dec: ast.AST) -> bool:
    """
    Matches:
      @asset
      @asset(...)
      @dagster.asset(...)
      @something.asset(...)
    """
    if isinstance(dec, ast.Name):
        return dec.id == "asset"
    if isinstance(dec, ast.Call):
        # @asset(...)
        fn = dec.func
        if isinstance(fn, ast.Name):
            return fn.id == "asset"
        if isinstance(fn, ast.Attribute):
            return fn.attr == "asset"
    if isinstance(dec, ast.Attribute):
        return dec.attr == "asset"
    return False


def _extract_code_version_from_decorator(dec: ast.AST) -> Optional[str]:
    """
    For @asset(..., code_version="v2", ...)
    """
    if not isinstance(dec, ast.Call):
        return None
    for kw in dec.keywords:
        if kw.arg != "code_version":
            continue
        val = kw.value
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            return val.value
        # allow simple Name? (discourage, but don’t crash)
        return None
    return None


def _fingerprint_function_body(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """
    Create a stable representation of the *function body* (not decorator).
    We purposely ignore:
      - line numbers, col offsets
      - docstring formatting differences (but docstring content changes will register)
    """
    # Build a shallow module containing only the function body statements
    # (including the docstring Expr if present).
    mod = ast.Module(body=fn.body, type_ignores=[])
    ast.fix_missing_locations(mod)
    return ast.dump(mod, annotate_fields=True, include_attributes=False)


def _parse_assets_from_source(rel_path: Path, source: str) -> Dict[str, AssetFn]:
    """
    Returns: function_name -> AssetFn
    Only includes functions decorated with @asset.
    """
    tree = ast.parse(source, filename=str(rel_path))
    out: Dict[str, AssetFn] = {}

    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        asset_decs = [d for d in node.decorator_list if _is_asset_decorator(d)]
        if not asset_decs:
            continue

        # If multiple asset-like decorators exist, prefer the first Call form that might contain code_version
        code_version: Optional[str] = None
        for d in asset_decs:
            cv = _extract_code_version_from_decorator(d)
            if cv is not None:
                code_version = cv
                break

        out[node.name] = AssetFn(
            file=rel_path,
            name=node.name,
            code_version=code_version,
            body_fingerprint=_fingerprint_function_body(node),
        )

    return out


def _read_file_at_head(rel_path: Path) -> Optional[str]:
    # git show HEAD:path
    return _git_maybe("show", f"HEAD:{rel_path.as_posix()}")


def _read_staged_file(rel_path: Path) -> Optional[str]:
    # git show :path
    return _git_maybe("show", f":{rel_path.as_posix()}")


def _rel(p: Path) -> Path:
    return p.relative_to(REPO_ROOT)


def main() -> int:
    failures: List[str] = []

    # Scan current working tree for all asset functions
    current_assets: Dict[Tuple[Path, str], AssetFn] = {}
    for abs_path in _python_files_in_repo():
        rel_path = _rel(abs_path)
        staged_src = _read_staged_file(rel_path)
        if staged_src is None:
            # File not staged → ignore it
            continue
        for fn_name, af in _parse_assets_from_source(rel_path, staged_src).items():
            current_assets[(rel_path, fn_name)] = af

    # For each current asset, compare to HEAD version (if it existed)
    for (rel_path, fn_name), cur in sorted(
        current_assets.items(), key=lambda x: (str(x[0][0]), x[0][1])
    ):
        # if cur.code_version is None:
        #     failures.append(
        #         f"{rel_path}:{fn_name}: missing required code_version=... in @asset decorator"
        #     )
        head_src = _read_file_at_head(rel_path)
        if head_src is None:
            # New file or not in HEAD: allow (but enforce code_version exists)
            if cur.code_version is None:
                failures.append(
                    f"{rel_path}:{fn_name}: missing code_version=... on new @asset"
                )
            continue

        head_assets = _parse_assets_from_source(rel_path, head_src)
        prev = head_assets.get(fn_name)
        if prev is None:
            # New function in existing file: allow (but enforce code_version exists)
            if cur.code_version is None:
                failures.append(
                    f"{rel_path}:{fn_name}: missing code_version=... on new @asset"
                )
            continue

        # If function body changed, require code_version differs
        if cur.body_fingerprint != prev.body_fingerprint:
            if cur.code_version is None:
                failures.append(
                    f"{rel_path}:{fn_name}: function body changed but code_version is missing"
                )
            elif prev.code_version is None:
                # Previously missing, now present: OK
                pass
            elif cur.code_version == prev.code_version:
                failures.append(
                    f"{rel_path}:{fn_name}: function body changed but code_version did not change "
                    f"(still '{cur.code_version}')"
                )

    if failures:
        print("[dagster code_version] ERROR:", file=sys.stderr)
        for msg in failures:
            print(f"  - {msg}", file=sys.stderr)
        print(
            "\nFix: bump code_version=... on the affected @asset decorators "
            "(it only needs to be different from HEAD).",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
