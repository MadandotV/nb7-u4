#!/usr/bin/env python3
"""Random nasty datasets: the fast binary must write exactly what the slow one does.

Usage: fuzz.py ROUNDS [SEED]
"""
import os, random, shutil, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "vendor/starter/starter-duckdb/scripts"))
from run_fixtures import SCHEMAS, load_actual, rows

ROOT = Path(__file__).resolve().parent.parent
FAST, SLOW = ROOT / "target/release/shopsess", ROOT / "target/release/reference"
START = 1791072000  # 2026-10-04T00:00:00Z


def iso(t, sep="T", z="Z"):
    import datetime
    d = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
    return d.strftime(f"%Y-%m-%d{sep}%H:%M:%S") + z


def ts(r, t_ms, bad_rate):
    t = t_ms // 1000
    if r.random() < bad_rate:
        return r.choice([
            "", iso(t, " "), iso(t, "t"), iso(t, "T", "z"), iso(t, "T", ""), f"{t_ms}.0", "-5", "1.79e12",
            iso(t)[:5] + "13" + iso(t)[7:], iso(t)[:5] + "02-30" + iso(t)[10:], iso(t)[:11] + "24" + iso(t)[13:],
            iso(t)[:17] + "60Z", iso(t, "T", "+24:00"), iso(t, "T", "+05:60"), iso(t, "T", "+0200"),
            iso(t) + "\r", iso(t) + " ", " " + iso(t), "2027-02-29T10:00:00Z", "86400000", "9" * 25, "0" * 20,
            iso(t, "T", ".000Z"), iso(t)[:19], "٢٥٠", iso(t).replace("-", "/"), "Z", "T", iso(t)[:10],
        ])
    form = r.random()
    if t_ms % 1000 or form < 0.3:
        return ("0" * r.choice([0, 0, 0, 3])) + str(t_ms)
    if form < 0.45:
        off = r.choice([-600, -570, -1, 0, 1, 330, 345, 720, 1439, -1439])
        sign = "-" if off < 0 or (off == 0 and r.random() < 0.5) else "+"
        return iso(t + off * 60, "T", f"{sign}{abs(off) // 60:02}:{abs(off) % 60:02}")
    return iso(t)


def line(r, fields, rates):
    out = []
    for f in fields:
        out.append(f'"{f}"' if r.random() < rates["quote"] else f)
    text = ",".join(out)
    if r.random() < rates["malformed"]:
        k = r.randrange(11)
        if k == 0: text = text.rsplit(",", 1)[0]
        elif k == 1: text += ",x"
        elif k == 2: text = '"' + text
        elif k == 3: text += '"'
        elif k == 4: text = ""
        elif k == 5:
            at = r.randrange(len(text) + 1); text = text[:at] + '"' + text[at:]
        elif k == 6:
            at = r.randrange(len(text) + 1); text = text[:at] + '""' + text[at:]
        elif k == 7: text = text.replace(",", ',"', 1)
        elif k == 8: text = text.replace(",", '",', 1)
        elif k == 9:
            parts = text.split(","); parts[r.randrange(len(parts))] = '"'; text = ",".join(parts)
        else:
            at = r.randrange(len(text) + 1); text = text[:at] + "," + text[at:]
    return text


def write(r, path, header, lines):
    r.shuffle(lines)
    body = "\n".join([header] + lines)
    if r.random() < 0.8: body += "\n"
    # Pad some files so that their size sits right at a 64-byte or page boundary.
    data = body.encode("utf-8", "surrogateescape")
    path.write_bytes(data)


def dataset(r, out):
    for d in ("events", "clicks", "links"):
        (out / d).mkdir(parents=True)
    days = r.choice([1, 2, 14, 14, 20])
    start = START + r.choice([0, 0, 3600, 86399])
    (out / "manifest.json").write_text('{"window_start": "%s", "window_end": "%s"}' % (iso(start), iso(start + days * 86400)))
    cats = ["shoes", "books", "toys", "unknown", "Books"][: r.randint(1, 5)]
    prods = [f"p{i}" for i in range(1, r.randint(2, 30))] + r.choice([[], ["P1", "p01", "px", " p1", "p-long-product-identifier"]])
    (out / "products.csv").write_text("product_id,category\n" + "".join(f"{p},{r.choice(cats)}\n" for p in prods))
    camps = [f"c_{i}" for i in range(1, r.randint(2, 12))] + r.choice([[], ["C_1", "c_01", "camp", "c_1000000"]])
    (out / "campaigns.csv").write_text("campaign_id,channel\n" + "".join(f"{c},{r.choice(['search', 'social', 'email'])}\n" for c in camps))
    rates = {"quote": r.choice([0, 0.2, 0.5, 1.0]), "malformed": r.choice([0, 0.01, 0.1])}
    bad = r.choice([0, 0.02, 0.2])
    alphabet = r.choice(["0123456789abcdefghijklmnopqrstuvwxyz", "ab", "aAbB01", "a b\t_-é"])
    nusers = r.randint(1, 60)
    def uid():
        n = r.choice([1, 2, 5, 9, 10, 11, 16, 17, 30])
        return "".join(r.choice(alphabet) for _ in range(n))
    users = [uid() for _ in range(nusers)]
    def user():
        u = r.choice(users)
        k = r.random()
        return "" if k < bad / 4 else u.upper() if k < bad / 2 else u
    links = []
    for _ in range(r.randint(0, nusers * 2)):
        links.append(line(r, [user(), user()], rates))
    nevents = r.choice([5, 50, 400, 3000])
    events, eids, taken = [], [], set()
    for i in range(nevents):
        t = start * 1000 + r.randrange(days * 86400_000 // 10) * r.choice([1, 1000, 1000, 1000]) % (days * 86400_000)
        if r.random() < 0.3: t = t // 1000 * 1000
        if r.random() < bad: t = start * 1000 + r.choice([-1, -1000, days * 86400_000, days * 86400_000 - 1, 0])
        ingest = t // 1000 + r.choice([0, 5, 600, -3, 86400, 86401, 86399, 200000])
        if t % 1000: ingest += 1
        kind = r.choice(["view"] * 6 + ["cart"] * 2 + ["purchase"] * 3 + ["Purchase", "views", "", "cart ", "purchas", "purchasee"][: 1 + int(bad * 20)])
        price = r.choice(["", "100", "2500", "1", "10000000", "0250", "000000000000000000000001"] + ["0", "10000001", "19.99", "-5", "+5", "1e3", "２５０", " 7", "99999999999999999999999"][: int(bad * 40)])
        eid = "e" + uid() if r.random() < 0.9 else r.choice(eids or ["e1"])
        eids.append(eid)
        prod = r.choice(prods + ["p999", ""][: 1 + int(bad * 5)])
        # The rules promise that copies with equal ingest_ts are identical,
        # so two different rows never share an (event_id, ingest_ts).
        while (eid, ingest) in taken:
            ingest += 1
        taken.add((eid, ingest))
        fields = [eid, user(), ts(r, t, bad), iso(ingest), kind, prod, price]
        events.append(line(r, fields, rates))
        for _ in range(r.choice([0, 0, 0, 0, 1, 2])):
            later = list(fields)
            again = ingest + r.choice([0, 1, 60, 600, -30])
            if again != ingest:
                if (eid, again) in taken:
                    continue
                taken.add((eid, again))
                later[3] = iso(again)
                if r.random() < 0.3: later[1] = user()
                if r.random() < 0.3: later[2] = ts(r, t, 0.5)
            events.append(line(r, later, rates))
    clicks = []
    for i in range(r.choice([0, 5, 50, 800])):
        t = start * 1000 - 7 * 86400_000 + r.randrange((days + 7) * 86400) * 1000 + r.choice([0, 0, 1, 999])
        if r.random() < bad: t = start * 1000 + r.choice([-7 * 86400_000 - 1, -7 * 86400_000, days * 86400_000, days * 86400_000 - 1])
        fields = ["k" + uid() + r.choice(["", str(i)]) + f"x{i}", user(), r.choice(camps + ["c_999", ""][: 1 + int(bad * 5)]), ts(r, t, bad)]
        for _ in range(r.choice([1, 1, 1, 2, 3])):
            clicks.append(line(r, fields, rates))
    # Same-millisecond pairs and clicks pinned to event times.
    for kind, rows_, header in (("events", events, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"),
                               ("clicks", clicks, "click_id,user_id,campaign_id,ts"), ("links", links, "user_id_a,user_id_b")):
        nfiles = r.randint(1, 4)
        parts = [[] for _ in range(nfiles)]
        for row in rows_:
            parts[r.randrange(nfiles)].append(row)
        for n, part in enumerate(parts):
            write(r, out / kind / f"part-{n:05}.csv", header, part)
        if r.random() < 0.2:
            (out / kind / "part-empty.csv").write_bytes(b"")
        if r.random() < 0.2:
            (out / kind / "part-header.csv").write_text(header + r.choice(["", "\n"]))
        (out / kind / "notes.txt").write_text("not,a,csv\n")


def run(binary, data, out):
    p = subprocess.run([str(binary), str(data), str(out)], capture_output=True, text=True,
                       env=dict(os.environ, NCPU=str(random.choice([1, 2, 8])), SHOPSESS_QUIET="1"))
    if p.returncode != 0:
        raise RuntimeError(f"{binary.name} exit {p.returncode}: {p.stderr[-800:]}")
    return {t: rows(load_actual(out / t, s)) for t, s in SCHEMAS.items()}


def main():
    rounds, seed = int(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 0
    for n in range(rounds):
        r = random.Random(seed * 1_000_003 + n)
        work = Path(tempfile.mkdtemp(prefix="fuzz-"))
        dataset(r, work / "data")
        fast, slow = run(FAST, work / "data", work / "fast"), run(SLOW, work / "data", work / "slow")
        if fast != slow:
            keep = ROOT / "out" / f"fuzz-fail-{seed}-{n}"
            shutil.rmtree(keep, ignore_errors=True); shutil.copytree(work, keep)
            for t in SCHEMAS:
                a, b = set(fast[t]), set(slow[t])
                print(f"DIFF round {n} {t}: fast-only {sorted(a - b, key=str)[:3]} slow-only {sorted(b - a, key=str)[:3]}")
            print("kept", keep); sys.exit(1)
        shutil.rmtree(work)
        if n % 50 == 49: print(f"{n + 1} rounds ok", flush=True)
    print("all rounds match")


if __name__ == "__main__":
    main()
