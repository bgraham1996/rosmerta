#!/usr/bin/env python3
"""
Rosmerta installer.

Follows the stub's plan:
  1. Check if there is already a .venv.
  2. Sync (and prepare) the .venv via `uv sync`.
  3. Check ~/.zshrc for a `rosmerta` alias.
  4. If an alias exists, report what it is aliased to.
  5. If not, suggest the alias line to add.

Stdlib-only on purpose — this may run before the project's dependencies are
installed. Run it with:

    uv run install.py      # or: python3 install.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_DIR = PROJECT_ROOT / ".venv"
VENV_PYTHON = VENV_DIR / "bin" / "python"
MAIN_PY = PROJECT_ROOT / "main.py"
ZSHRC = Path.home() / ".zshrc"
ALIAS_NAME = "rosmerta"


# check if there is already a .venv
def venv_exists() -> bool:
    return VENV_DIR.exists()


# sync and activate .venv
def sync_venv() -> bool:
    """Create/refresh .venv via `uv sync`. Returns True on success.

    Note: a subprocess can't "activate" a venv for your interactive shell —
    that's what the alias below is for. `uv sync` is what makes the venv
    usable, so that's the concrete step here.
    """
    if venv_exists():
        print(f"✓ Found existing virtualenv at {VENV_DIR}")
    else:
        print(f"• No .venv yet — creating one at {VENV_DIR}")

    if shutil.which("uv") is None:
        print("✗ `uv` is not on PATH. Install it first: https://docs.astral.sh/uv/")
        return False

    print("• Running `uv sync` to install/refresh dependencies...")
    result = subprocess.run(["uv", "sync"], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print("✗ `uv sync` failed — see output above.")
        return False

    if not VENV_PYTHON.exists():
        print(f"✗ Expected interpreter not found at {VENV_PYTHON} after sync.")
        return False

    print(f"✓ Environment ready ({VENV_PYTHON})")
    return True


# check .zshrc file for 'rosmerta' alias
def find_alias() -> str | None:
    """Return the raw `alias rosmerta=...` line from ~/.zshrc, or None."""
    if not ZSHRC.exists():
        return None
    for raw in ZSHRC.read_text().splitlines():
        line = raw.strip()
        if line.startswith(f"alias {ALIAS_NAME}="):
            return line
    return None


def suggested_alias() -> str:
    # "alias rosmerta='~/your path/.venv/bin/python ~/your path/main.py'"
    return f"alias {ALIAS_NAME}='{VENV_PYTHON} {MAIN_PY}'"


def check_alias() -> None:
    existing = find_alias()
    if existing:
        # if there is an alias, report what command is being aliased
        print(f"✓ A `{ALIAS_NAME}` alias is already defined in {ZSHRC}:")
        print(f"    {existing}")
        print("  If it points somewhere else, update it to:")
        print(f"    {suggested_alias()}")
    else:
        # if there is no alias, suggest the line to add
        print(f"• No `{ALIAS_NAME}` alias found in {ZSHRC}.")
        print("  Add this line, then run `source ~/.zshrc`:")
        print(f"\n    {suggested_alias()}\n")


def main() -> int:
    print("Rosmerta installer\n" + "=" * 18)
    if not sync_venv():
        return 1
    print()
    check_alias()
    print("\nDone. Verify with:  rosmerta --help")
    return 0


if __name__ == "__main__":
    sys.exit(main())
