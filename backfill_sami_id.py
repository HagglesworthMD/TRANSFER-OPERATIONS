"""One-time backfill: populate blank sami_id in daily_stats_v2.csv from Subject lines."""

import csv
import os
import re
import tempfile

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_stats_v2.csv")
_RE_SAMI = re.compile(r"\bSAMI-[A-Z0-9]+\b", re.IGNORECASE)
SUBJECT_COL = 2
SAMI_ID_COL = 16


def backfill():
    if not os.path.isfile(CSV_PATH):
        print(f"File not found: {CSV_PATH}")
        return

    total = 0
    patched = 0
    already_set = 0
    no_sami_in_subject = 0

    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".csv", dir=os.path.dirname(CSV_PATH)
    )
    try:
        with (
            open(CSV_PATH, "r", newline="", encoding="utf-8") as fin,
            os.fdopen(tmp_fd, "w", newline="", encoding="utf-8") as fout,
        ):
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            header = next(reader, None)
            if header:
                writer.writerow(header)

            for row in reader:
                total += 1
                if len(row) < 17:
                    writer.writerow(row)
                    continue

                existing = (row[SAMI_ID_COL] or "").strip()
                if existing:
                    already_set += 1
                    writer.writerow(row)
                    continue

                subject = row[SUBJECT_COL] if len(row) > SUBJECT_COL else ""
                m = _RE_SAMI.search(subject)
                if m:
                    row[SAMI_ID_COL] = m.group(0).upper().strip()
                    patched += 1
                else:
                    no_sami_in_subject += 1

                writer.writerow(row)

        os.replace(tmp_path, CSV_PATH)
        tmp_path = None  # prevent cleanup
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    print(f"Backfill complete.")
    print(f"  Total data rows:    {total}")
    print(f"  Already had sami_id:{already_set}")
    print(f"  Patched:            {patched}")
    print(f"  No SAMI in subject: {no_sami_in_subject}")


if __name__ == "__main__":
    backfill()
