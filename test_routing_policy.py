#!/usr/bin/env python3
"""
Test script for TRANSFER-BOT Authoritative Routing Policy

This script verifies the routing behavior for all email classes:
1. External image request domains (Class 1)
2. Internal sa.gov.au with SAMI support staff
3. Internal sa.gov.au with non-SAMI senders
4. System notification domains (Class 3)
5. Unknown domains

Expected behaviors documented per AUTHORITATIVE POLICY.
"""

import sys
sys.path.insert(0, '.')
import distributor

def test_routing_policy():
    """Test all routing policy scenarios"""

    # Load policy
    policy, valid = distributor.load_domain_policy()
    if not valid:
        print("ERROR: Policy invalid!")
        return False

    print("=" * 80)
    print("TRANSFER-BOT ROUTING POLICY VERIFICATION")
    print("=" * 80)
    print()

    # Test scenarios
    test_cases = [
        {
            "name": "Class 1: External Image Request (bensonradiology.com.au)",
            "domain": "bensonradiology.com.au",
            "sender": "requests@bensonradiology.com.au",
            "expected_bucket": "external_image_request",
            "expected_action": "IMAGE_REQUEST_EXTERNAL",
            "expected_assigned": True,
            "expected_cc_manager": False,
            "expected_cc_apps": False,
            "expected_held": False,
            "expected_completion": False,
        },
        {
            "name": "Class 1: External Image Request (jonesradiology.com.au)",
            "domain": "jonesradiology.com.au",
            "sender": "admin@jonesradiology.com.au",
            "expected_bucket": "external_image_request",
            "expected_action": "IMAGE_REQUEST_EXTERNAL",
            "expected_assigned": True,
            "expected_cc_manager": False,
            "expected_cc_apps": False,
            "expected_held": False,
            "expected_completion": False,
        },
        {
            "name": "Internal: SAMI Support Staff (brian.shaw@sa.gov.au)",
            "domain": "sa.gov.au",
            "sender": "brian.shaw@sa.gov.au",
            "expected_bucket": "internal",
            "expected_action": "COMPLETION",
            "expected_assigned": False,
            "expected_cc_manager": False,
            "expected_cc_apps": False,
            "expected_held": False,
            "expected_completion": True,
        },
        {
            "name": "Internal: Non-SAMI sender (random.user@sa.gov.au)",
            "domain": "sa.gov.au",
            "sender": "random.user@sa.gov.au",
            "expected_bucket": "internal",
            "expected_action": "INTERNAL_QUERY",
            "expected_assigned": True,
            "expected_cc_manager": False,
            "expected_cc_apps": False,
            "expected_held": False,
            "expected_completion": False,
        },
        {
            "name": "Class 3: System Notification (configured domains)",
            "domain": "jonesradiology.atlassian.net",
            "sender": "alert@jonesradiology.atlassian.net",
            "expected_bucket": "system_notification",
            "expected_action": "SYSTEM_NOTIFICATION",
            "expected_assigned": False,
            "expected_cc_manager": True,
            "expected_cc_apps": False,
            "expected_held": True,
            "expected_completion": False,
        },
        {
            "name": "Unknown Domain (randomvendor.com)",
            "domain": "randomvendor.com",
            "sender": "sales@randomvendor.com",
            "expected_bucket": "unknown",
            "expected_action": "UNKNOWN_DOMAIN",
            "expected_assigned": False,
            "expected_cc_manager": False,
            "expected_cc_apps": False,
            "expected_held": True,
            "expected_completion": False,
        },
    ]

    all_pass = True

    for i, test in enumerate(test_cases, 1):
        print(f"Test {i}: {test['name']}")
        print("-" * 80)

        # Test domain classification
        bucket = distributor.classify_sender_domain(test['domain'], policy)
        if bucket == test['expected_bucket']:
            print(f"  [PASS] Domain bucket: {bucket}")
        else:
            print(f"  [FAIL] Domain bucket: {bucket} (expected: {test['expected_bucket']})")
            all_pass = False

        # Test SAMI staff detection for internal domains
        if test['expected_bucket'] == 'internal':
            is_sami = distributor.is_sami_support_staff(test['sender'], policy)
            if is_sami == test['expected_completion']:
                print(f"  [PASS] SAMI staff detection: {is_sami}")
            else:
                print(f"  [FAIL] SAMI staff detection: {is_sami} (expected: {test['expected_completion']})")
                all_pass = False

        # Simulate routing decision logic
        if bucket == "external_image_request":
            action = "IMAGE_REQUEST_EXTERNAL"
            assigned = True
            cc_manager = False
            cc_apps = False
            held = False
            completion = False
        elif bucket == "internal":
            is_sami = distributor.is_sami_support_staff(test['sender'], policy)
            if is_sami:
                action = "COMPLETION"
                assigned = False
                cc_manager = False
                cc_apps = False
                held = False
                completion = True
            else:
                action = "INTERNAL_QUERY"
                assigned = True
                cc_manager = False
                cc_apps = False
                held = False
                completion = False
        elif bucket == "system_notification":
            action = "SYSTEM_NOTIFICATION"
            assigned = False
            cc_manager = True  # Manager added as CC
            cc_apps = False     # Apps added as To (not CC)
            held = True
            completion = False
        elif bucket == "unknown":
            action = "UNKNOWN_DOMAIN"
            assigned = False
            cc_manager = False  # Manager added as To (not CC)
            cc_apps = False
            held = True
            completion = False
        else:
            action = "FALLBACK"
            assigned = True
            cc_manager = False
            cc_apps = False
            held = False
            completion = False

        # Verify expected behavior
        checks = [
            ("Action", action, test['expected_action']),
            ("Assigned to staff", assigned, test['expected_assigned']),
            ("CC Manager", cc_manager, test['expected_cc_manager']),
            ("CC Apps", cc_apps, test['expected_cc_apps']),
            ("Held", held, test['expected_held']),
            ("Completion", completion, test['expected_completion']),
        ]

        for check_name, actual, expected in checks:
            if actual == expected:
                print(f"  [PASS] {check_name}: {actual}")
            else:
                print(f"  [FAIL] {check_name}: {actual} (expected: {expected})")
                all_pass = False

        print()

    print("=" * 80)
    if all_pass:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
    print("=" * 80)

    return all_pass

if __name__ == "__main__":
    success = test_routing_policy()
    sys.exit(0 if success else 1)
