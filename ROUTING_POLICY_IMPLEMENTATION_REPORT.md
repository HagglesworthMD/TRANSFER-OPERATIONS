# TRANSFER-BOT Authoritative Routing Policy - Implementation Report

## Summary

Implemented domain-aware routing with explicit CC behavior per the Authoritative Policy:
- **Class 1**: External image request domains → round-robin to staff, NO CC to manager/apps
- **Internal sa.gov.au with SAMI staff**: Treat as COMPLETION, no redistribution
- **Internal sa.gov.au non-SAMI**: Round-robin to staff
- **Class 3**: System notifications → CC apps + manager, NOT assigned to staff
- **Unknown domains**: Hold + CC manager only

## Files Changed

### 1. domain_policy.json (Configuration)
**Changed:** Complete restructure to include authoritative routing policy

**New structure:**
```json
{
  "internal_domains": ["sa.gov.au"],
  "external_image_request_domains": [
    "bensonradiology.com.au",
    "jonesradiology.com.au",
    "radiologysa.com.au",
    "drjones.com.au",
    "i-med.com.au"
  ],
  "system_notification_domains": [],
  "sami_support_staff": [
    "christina.carroll@sa.gov.au",
    "hannah.cutting@sa.gov.au",
    "deepak.devarapalli@sa.gov.au",
    "john.drousas@sa.gov.au",
    "debbie.fowell@sa.gov.au",
    "ajumeet.kaur@sa.gov.au",
    "prav.mudaliar@sa.gov.au",
    "kerry.murphy@sa.gov.au",
    "muiru.mutuota@sa.gov.au",
    "gioia.perre@sa.gov.au",
    "craig.ravlich@sa.gov.au",
    "brian.shaw@sa.gov.au"
  ],
  "apps_specialists": [
    "tony.penna@sa.gov.au",
    "kate.cook@sa.gov.au"
  ],
  "manager_email": "Jason.quinn2@sa.gov.au"
}
```

### 2. distributor.py (Code Changes)

**Changed sections:**

#### a) load_domain_policy() (lines 531-603)
- Updated to accept new optional fields (external_image_request_domains, system_notification_domains, sami_support_staff, apps_specialists, manager_email)
- Maintains backward compatibility with old 3-key policy format
- Validates all list fields
- Provides safe defaults for missing optional fields

#### b) classify_sender_domain() (lines 605-637)
- Returns more specific buckets: `external_image_request`, `system_notification`, `internal`, `hold`, `unknown`
- Checks external image request domains first (highest priority)
- Checks system notification domains second
- Falls back to internal → hold → unknown

#### c) is_sami_support_staff() (lines 639-650) - NEW FUNCTION
- Checks if sender email is in SAMI support staff list
- Case-insensitive matching
- Returns boolean

#### d) Routing logic in process_inbox() (lines 1361-1457)
- Complete rewrite of routing decision logic
- Implements all 5 routing classes with explicit CC behavior:

  **external_image_request:**
  - Assign to staff via round-robin
  - cc_manager = False, cc_apps = False
  - action = "IMAGE_REQUEST_EXTERNAL"

  **internal + SAMI staff:**
  - Treat as COMPLETION
  - Mark as completed, no reassignment
  - Early exit from processing loop
  - action = "COMPLETION"

  **internal + non-SAMI:**
  - Assign to staff via round-robin
  - cc_manager = False, cc_apps = False
  - action = "INTERNAL_QUERY"

  **system_notification:**
  - Do NOT assign to staff
  - Add manager + all apps specialists to hold_recipients
  - cc_manager = True, cc_apps = True
  - action = "SYSTEM_NOTIFICATION"

  **unknown:**
  - Hold (do not assign to staff)
  - Add manager to hold_recipients
  - cc_manager = True, cc_apps = False
  - action = "UNKNOWN_DOMAIN"

#### e) Forward email section (lines 1464-1493)
- Uses hold_recipients for HOLD and SYSTEM_NOTIFICATION routing
- Uses assignee for normal staff routing
- Adds CC recipients based on cc_manager and cc_apps flags
- Gets addresses from policy (policy_manager, policy_apps_specialists) instead of settings_overrides

## Diff Summary

**domain_policy.json:**
- Added 5 external_image_request_domains
- Added 12 sami_support_staff emails
- Added 2 apps_specialists emails
- Added manager_email
- Added system_notification_domains (empty, ready for future use)

**distributor.py:**
- ~170 lines modified/added
- 1 new function (is_sami_support_staff)
- Updated policy validation logic
- Updated domain classification logic
- Complete routing logic replacement
- Updated forward/CC logic

## How to Test

### Method 1: Automated Test Script
```bash
cd /c/codex-test/TRANSFER-BOT-main
python test_routing_policy.py
```

Expected: All tests PASS except Test 5 (system_notification - no domains configured yet)

### Method 2: Manual Classification Test
```python
python -c "
import sys; sys.path.insert(0, '.')
import distributor

policy, valid = distributor.load_domain_policy()

# Test external image request
print(distributor.classify_sender_domain('bensonradiology.com.au', policy))
# Expected: external_image_request

# Test SAMI staff
print(distributor.is_sami_support_staff('brian.shaw@sa.gov.au', policy))
# Expected: True

# Test unknown
print(distributor.classify_sender_domain('randomvendor.com', policy))
# Expected: unknown
"
```

### Method 3: Live Email Processing (if available)
1. Send test email from one of the 5 external image domains
2. Check daily_stats.csv for:
   - Domain Bucket = external_image_request
   - Action = IMAGE_REQUEST_EXTERNAL
   - Assigned To = <staff email>
3. Verify Jason.quinn2@sa.gov.au is NOT in CC
4. Verify apps specialists are NOT in CC

## Verification Checklist

- [x] Policy loads successfully
- [x] External image request domains classified correctly
- [x] SAMI support staff detection works
- [x] Internal non-SAMI senders assigned to staff
- [x] Unknown domains held with manager CC
- [x] No crashes, no syntax errors
- [x] Audit logging includes new columns (Domain Bucket, Action, Policy Source)
- [x] All decisions logged explicitly
- [x] Backward compatible with old daily_stats.csv schema

## Acceptance Criteria Verification

| Criteria | Status | Evidence |
|----------|--------|----------|
| External image requests → staff, NO CC manager | ✅ PASS | test_routing_policy.py Tests 1-2 |
| System notifications → CC apps + manager, no staff | ⚠️ N/A | No domains configured yet (empty list) |
| Unknown domains → held, CC manager only | ✅ PASS | test_routing_policy.py Test 6 |
| Internal non-SAMI → staff | ✅ PASS | test_routing_policy.py Test 4 |
| Internal SAMI → completion | ✅ PASS | test_routing_policy.py Test 3 |
| All decisions logged | ✅ PASS | Explicit log calls in routing logic |
| No crashes | ✅ PASS | py_compile, test execution |

## Rollback Procedure

### Quick Rollback (Configuration Only)
```bash
cd /c/codex-test/TRANSFER-BOT-main
git checkout domain_policy.json
```

This will restore the old 3-key policy format. distributor.py will still work (backward compatible).

### Full Rollback (Code + Configuration)
```bash
cd /c/codex-test/TRANSFER-BOT-main
git checkout distributor.py domain_policy.json
rm test_routing_policy.py
```

### Restore Old Behavior
If you need the old vendor CC behavior back:
1. Add domains to `vendor_domains` in domain_policy.json
2. The old routing logic will be triggered for "vendor" bucket

## Risks & Assumptions

### Risks
1. **SAMI staff list accuracy**: If list is incomplete, legitimate completions may be routed as new work
2. **External image domain list**: New imaging partners not in list will be treated as unknown
3. **Manager email typo**: Would prevent CC from working (logged but email won't send)

### Assumptions
1. **SAMI support staff list is authoritative**: Any email from these addresses to sa.gov.au is a completion
2. **External image requests don't need manager oversight**: Policy decision to reduce CC noise
3. **System notification domains**: Empty for now, ready for future Class 3 senders
4. **Dashboard reads CSV correctly**: No dashboard code changes needed

### Mitigations
1. **Policy validation**: Strict validation on load, fail-safe defaults
2. **Explicit logging**: Every routing decision logged with bucket and CC flags
3. **Audit trail**: daily_stats.csv captures all routing metadata
4. **No silent failures**: All errors logged, bot continues processing

## Notes

- **Manager email is in policy**: Changed from settings_overrides.json to domain_policy.json for consistency
- **Apps specialists in policy**: Centralized all routing policy in one file
- **No dashboard changes**: Dashboard just reads the CSV with new columns
- **Backward compatible**: Old daily_stats.csv files still work (6-column mode)
- **Atomic writes**: All policy changes use atomic JSON writes
- **No email deletion**: Original constraint maintained
- **Local-only**: No external dependencies added

## Future Enhancements

1. **Add Class 3 system notification domains** when identified
2. **Expand external image request list** as new partners are added
3. **Update SAMI staff list** when team changes occur
4. **Dashboard filters** by Domain Bucket for reporting
5. **Alert on unknown domains** after X occurrences for policy review

---

**Implementation Date:** 2026-01-19
**Author:** Junior Dev (Codex CLI)
**Status:** ✅ Complete and Tested
