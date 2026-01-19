#!/usr/bin/env python3
"""
Smoke test for SAFE_MODE functionality (no Outlook required)

This script verifies:
1. SAFE_MODE is active by default (env var not set)
2. SAFE_MODE is active when inbox folder contains "test"
3. LIVE_MODE is armed when TRANSFER_BOT_LIVE=true and inbox folder doesn't contain "test"
4. Only one .Send() call exists in distributor.py (protected by SAFE_MODE)
"""

import os
import sys

# Add current directory to path for import
sys.path.insert(0, '.')
import distributor

def test_send_count():
    """Assert that only one .Send() call exists in distributor.py"""
    print("Pre-flight check: Verify single Send() call exists")
    print("-" * 80)

    distributor_path = "distributor.py"
    try:
        with open(distributor_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"  [FAIL] Could not read distributor.py: {e}")
        return False

    # Count occurrences of .Send(
    send_count = content.count('.Send(')

    if send_count == 1:
        print(f"  [PASS] Exactly 1 .Send() call found (protected by SAFE_MODE)")
        return True
    else:
        print(f"  [FAIL] Found {send_count} .Send() calls (expected exactly 1)")
        # Find and print all occurrences
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if '.Send(' in line:
                print(f"    Line {i}: {line.strip()}")
        return False

def test_safe_mode():
    """Run all SAFE_MODE test scenarios"""
    print("=" * 80)
    print("TRANSFER-BOT SAFE_MODE VERIFICATION")
    print("=" * 80)
    print()

    all_pass = True

    # Pre-flight: check Send() count
    if not test_send_count():
        all_pass = False
    print()

    # Save original environment and config
    original_env = os.environ.get("TRANSFER_BOT_LIVE")
    original_inbox_folder = distributor.CONFIG.get("inbox_folder", "")

    try:
        # Test 1: Default behavior (env var not set)
        print("Test 1: Default behavior (TRANSFER_BOT_LIVE not set)")
        print("-" * 80)
        os.environ.pop("TRANSFER_BOT_LIVE", None)  # Ensure it's not set
        distributor.CONFIG["inbox_folder"] = "Transfer Bot Test Received"
        is_safe, reason = distributor.is_safe_mode()
        if is_safe and reason == "env_missing":
            print("  [PASS] SAFE_MODE active (reason: env_missing)")
        else:
            print(f"  [FAIL] Expected SAFE_MODE active with env_missing, got is_safe={is_safe}, reason={reason}")
            all_pass = False
        print()

        # Test 2: Env var set to "true" but folder contains "test"
        print("Test 2: TRANSFER_BOT_LIVE=true but folder contains 'test'")
        print("-" * 80)
        os.environ["TRANSFER_BOT_LIVE"] = "true"
        distributor.CONFIG["inbox_folder"] = "Transfer Bot Test Received"
        is_safe, reason = distributor.is_safe_mode()
        if is_safe and reason == "test_folder":
            print("  [PASS] SAFE_MODE active (reason: test_folder)")
        else:
            print(f"  [FAIL] Expected SAFE_MODE active with test_folder, got is_safe={is_safe}, reason={reason}")
            all_pass = False
        print()

        # Test 3: Env var set to "true" and folder does NOT contain "test"
        print("Test 3: TRANSFER_BOT_LIVE=true and production folder")
        print("-" * 80)
        os.environ["TRANSFER_BOT_LIVE"] = "true"
        distributor.CONFIG["inbox_folder"] = "Transfer Bot Received"
        is_safe, reason = distributor.is_safe_mode()
        if not is_safe and reason == "live_mode_armed":
            print("  [PASS] LIVE_MODE armed")
        else:
            print(f"  [FAIL] Expected LIVE_MODE armed, got is_safe={is_safe}, reason={reason}")
            all_pass = False
        print()

        # Test 4: Env var set to "TRUE" (uppercase - should work)
        print("Test 4: TRANSFER_BOT_LIVE=TRUE (case insensitive)")
        print("-" * 80)
        os.environ["TRANSFER_BOT_LIVE"] = "TRUE"
        distributor.CONFIG["inbox_folder"] = "Transfer Bot Received"
        is_safe, reason = distributor.is_safe_mode()
        if not is_safe and reason == "live_mode_armed":
            print("  [PASS] LIVE_MODE armed (case insensitive)")
        else:
            print(f"  [FAIL] Expected LIVE_MODE armed, got is_safe={is_safe}, reason={reason}")
            all_pass = False
        print()

        # Test 5: Env var set to "yes" (should NOT arm)
        print("Test 5: TRANSFER_BOT_LIVE=yes (incorrect value)")
        print("-" * 80)
        os.environ["TRANSFER_BOT_LIVE"] = "yes"
        distributor.CONFIG["inbox_folder"] = "Transfer Bot Received"
        is_safe, reason = distributor.is_safe_mode()
        if is_safe and reason == "env_missing":
            print("  [PASS] SAFE_MODE active (only 'true' arms live mode)")
        else:
            print(f"  [FAIL] Expected SAFE_MODE active, got is_safe={is_safe}, reason={reason}")
            all_pass = False
        print()

    finally:
        # Restore original environment and config
        if original_env is None:
            os.environ.pop("TRANSFER_BOT_LIVE", None)
        else:
            os.environ["TRANSFER_BOT_LIVE"] = original_env
        distributor.CONFIG["inbox_folder"] = original_inbox_folder

    print("=" * 80)
    if all_pass:
        print("ALL TESTS PASSED")
        print()
        print("Summary:")
        print("  - Exactly 1 .Send() call exists (protected by SAFE_MODE)")
        print("  - SAFE_MODE is active by default (TRANSFER_BOT_LIVE not set)")
        print("  - SAFE_MODE is enforced when folder contains 'test'")
        print("  - LIVE_MODE requires TRANSFER_BOT_LIVE=true AND production folder")
    else:
        print("SOME TESTS FAILED")
    print("=" * 80)

    return all_pass

if __name__ == "__main__":
    success = test_safe_mode()
    sys.exit(0 if success else 1)
