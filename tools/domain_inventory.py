#!/usr/bin/env python3
"""
Domain Inventory Report Generator
Analyzes outlook snapshot to produce domain-aware routing inventory.

Usage:
    python tools/domain_inventory.py "INBOX SCRAPE/outlook_snapshot_health.samisupportteam_FULL_20260116-1227.csv"

Output:
    domain_inventory_report.csv
    domain_inventory_report.json

Constraints:
- Local-only, no external calls
- NO email addresses or names in output (domains only)
- Deterministic bucketing heuristics
"""

import sys
import csv
import json
import re
from datetime import datetime
from collections import defaultdict
from pathlib import Path


def parse_email_domain(email_str):
    """
    Extract domain from email address.
    Handles: "Name <user@domain>" and bare "user@domain"
    Returns domain or None
    """
    if not email_str:
        return None

    email_str = str(email_str).strip()

    # Handle "Name <user@domain>" format
    match = re.search(r'<([^>]+)>', email_str)
    if match:
        email_str = match.group(1)

    # Extract domain from email
    if '@' in email_str:
        try:
            domain = email_str.split('@')[-1].strip().lower()
            return domain if domain else None
        except Exception:
            return None

    return None


def classify_bucket(domain, count):
    """
    Heuristic for suggested_bucket:
    - sa.gov.au => internal
    - count >= 25 => vendor_candidate
    - count == 1 => one_off
    - else => other
    """
    if domain == "sa.gov.au":
        return "internal"
    elif count >= 25:
        return "vendor_candidate"
    elif count == 1:
        return "one_off"
    else:
        return "other"


def parse_received_time(time_str):
    """Parse received_time with robust handling"""
    if not time_str:
        return None

    try:
        # Handle ISO format with timezone
        if '+' in time_str or time_str.endswith('Z'):
            return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        # Try plain ISO format
        return datetime.fromisoformat(time_str)
    except Exception:
        return None


def analyze_snapshot(csv_path):
    """
    Read snapshot CSV and extract domain inventory.
    Returns dict: {domain: {count, first_seen, last_seen}}
    """
    inventory = defaultdict(lambda: {
        'count': 0,
        'first_seen': None,
        'last_seen': None,
        'received_times': []
    })

    # Possible column names for sender email (try in order)
    email_column_candidates = [
        'sender_email',
        'from_email',
        'from',
        'sender',
        'mail_from',
        'sender_domain'  # Already parsed domain
    ]

    received_time_candidates = [
        'received_time',
        'receivedtime',
        'received',
        'date'
    ]

    try:
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Find email column
            email_col = None
            for candidate in email_column_candidates:
                if candidate in headers:
                    email_col = candidate
                    break

            if not email_col:
                print(f"ERROR: No sender email column found. Available columns: {headers}")
                return {}

            # Find received time column
            time_col = None
            for candidate in received_time_candidates:
                if candidate in headers:
                    time_col = candidate
                    break

            print(f"Using email column: {email_col}")
            if time_col:
                print(f"Using time column: {time_col}")

            row_count = 0
            parsed_count = 0

            for row in reader:
                row_count += 1

                # Extract domain
                if email_col == 'sender_domain':
                    # Already a domain
                    domain = (row.get(email_col) or '').strip().lower()
                else:
                    # Parse from email
                    email_value = row.get(email_col, '')
                    domain = parse_email_domain(email_value)

                if not domain:
                    continue

                parsed_count += 1

                # Extract received time
                received_time = None
                if time_col:
                    time_str = row.get(time_col, '')
                    received_time = parse_received_time(time_str)

                # Update inventory
                inventory[domain]['count'] += 1
                if received_time:
                    inventory[domain]['received_times'].append(received_time)

            print(f"Processed {row_count} rows, extracted {parsed_count} domains")

    except FileNotFoundError:
        print(f"ERROR: File not found: {csv_path}")
        return {}
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        return {}

    # Compute first_seen and last_seen for each domain
    final_inventory = {}
    for domain, data in inventory.items():
        times = data['received_times']
        if times:
            first_seen = min(times).isoformat()
            last_seen = max(times).isoformat()
        else:
            first_seen = None
            last_seen = None

        count = data['count']
        suggested_bucket = classify_bucket(domain, count)

        final_inventory[domain] = {
            'domain': domain,
            'count': count,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'suggested_bucket': suggested_bucket
        }

    return final_inventory


def write_reports(inventory, output_dir='.'):
    """Write CSV and JSON reports"""
    output_dir = Path(output_dir)
    csv_path = output_dir / 'domain_inventory_report.csv'
    json_path = output_dir / 'domain_inventory_report.json'

    # Sort by count descending
    sorted_domains = sorted(
        inventory.values(),
        key=lambda x: (-x['count'], x['domain'])
    )

    # Write CSV
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'domain', 'count', 'first_seen', 'last_seen', 'suggested_bucket'
            ])
            writer.writeheader()
            for record in sorted_domains:
                writer.writerow(record)
        print(f"[OK] Written: {csv_path}")
    except Exception as e:
        print(f"ERROR writing CSV: {e}")
        return False

    # Write JSON
    try:
        # Convert to dict keyed by domain for JSON
        json_data = {rec['domain']: rec for rec in sorted_domains}
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)
        print(f"[OK] Written: {json_path}")
    except Exception as e:
        print(f"ERROR writing JSON: {e}")
        return False

    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/domain_inventory.py <snapshot.csv>")
        print('Example: python tools/domain_inventory.py "INBOX SCRAPE/outlook_snapshot_health.samisupportteam_FULL_20260116-1227.csv"')
        sys.exit(1)

    csv_path = sys.argv[1]
    print(f"Analyzing snapshot: {csv_path}")

    inventory = analyze_snapshot(csv_path)

    if not inventory:
        print("ERROR: No domains extracted")
        sys.exit(1)

    print(f"\nExtracted {len(inventory)} unique domains")

    # Show top 10 domains
    sorted_domains = sorted(inventory.values(), key=lambda x: -x['count'])
    print("\nTop 10 domains:")
    for i, rec in enumerate(sorted_domains[:10], 1):
        print(f"  {i}. {rec['domain']}: {rec['count']} emails ({rec['suggested_bucket']})")

    # Write reports
    success = write_reports(inventory)

    if success:
        print("\n[SUCCESS] Domain inventory reports generated successfully")
        sys.exit(0)
    else:
        print("\n[FAILED] Failed to write reports")
        sys.exit(1)


if __name__ == '__main__':
    main()
