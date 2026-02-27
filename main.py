#!/usr/bin/env python3
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ENV_CANDIDATES = [ROOT / "POLYMARKET_ENV.sh", ROOT / ".env.polymarket"]
ANACONDA_PY = "/Users/killdead/opt/anaconda3/bin/python3"


def load_env_file() -> Path:
    for p in ENV_CANDIDATES:
        if p.exists():
            env_path = p
            break
    else:
        raise SystemExit("Missing POLYMARKET_ENV.sh or .env.polymarket")

    # Minimal parser for lines like: export KEY="value"
    for line in env_path.read_text(errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = re.match(r"^export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$", s)
        if not m:
            continue
        key, raw = m.group(1), m.group(2).strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
            val = raw[1:-1]
        else:
            val = raw
        os.environ[key] = val
    return env_path


def main() -> None:
    env_path = load_env_file()
    py = ANACONDA_PY if Path(ANACONDA_PY).exists() else sys.executable
    cmd = [py, str(ROOT / "btc5m_live_main.py"), "--probe-next"]

    print(f"[main-wrapper] env={env_path.name}", flush=True)
    print(f"[main-wrapper] python={py}", flush=True)
    print(f"[main-wrapper] cmd={' '.join(cmd)}", flush=True)

    rc = subprocess.call(cmd, cwd=str(ROOT), env=os.environ.copy())
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
