#!/usr/bin/env python3
"""Shrinks a failing fuzz dataset: drops lines while fast and slow still differ. Usage: shrink.py DIR"""
import os, shutil, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fuzz

src = Path(sys.argv[1]) / "data"
work = Path(tempfile.mkdtemp(prefix="shrink-"))
files = [p for k in ("events", "clicks", "links") for p in sorted((src / k).glob("*.csv"))]
content = {p: p.read_bytes().split(b"\n") for p in files}

def differs(cur):
    d = work / "data"; shutil.rmtree(work, ignore_errors=True); shutil.copytree(src, d)
    for p, lines in cur.items():
        (d / p.relative_to(src)).write_bytes(b"\n".join(lines))
    try:
        return fuzz.run(fuzz.FAST, d, work / "fast") != fuzz.run(fuzz.SLOW, d, work / "slow")
    except RuntimeError as e:
        return True

assert differs(content)
for p in files:
    lines = content[p]; n = max(1, (len(lines) - 1) // 2)
    while n >= 1:
        i = 1
        while i < len(lines):
            trial = dict(content); trial[p] = lines[:i] + lines[i + n:]
            if differs(trial):
                lines = trial[p]; content[p] = lines
            else:
                i += n
        n //= 2
for p in files:
    print(p.relative_to(src), content[p])
print((src / "manifest.json").read_text())
