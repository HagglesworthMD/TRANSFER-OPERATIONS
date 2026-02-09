"""Read/write staff.txt — the ONLY file this dashboard writes to."""

import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _lock_file(f):
    """Platform-aware file locking."""
    if sys.platform == "win32":
        import msvcrt
        try:
            # Lock a reasonably large byte range
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 4096)
        except OSError:
            # If locking fails, log but continue (file closing will release any locks)
            logger.debug("File lock acquire failed (non-fatal)")
    else:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)


def _unlock_file(f):
    if sys.platform == "win32":
        import msvcrt
        try:
            # Unlock the same byte range
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 4096)
        except OSError:
            # Unlock errors are non-fatal (file close releases locks anyway)
            logger.debug("File lock release failed (non-fatal)")
    else:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def read_staff(path: Path) -> list[str]:
    """Return list of staff emails from file."""
    try:
        text = path.read_text(encoding="utf-8")
        return [line.strip() for line in text.splitlines() if line.strip()]
    except FileNotFoundError:
        logger.warning("staff.txt not found at %s", path)
        return []
    except Exception:
        logger.exception("Error reading staff.txt")
        return []


def _write_staff(path: Path, emails: list[str]) -> None:
    """Write staff list back to file with locking."""
    content = "\n".join(emails) + "\n"
    with open(path, "r+", encoding="utf-8") as f:
        _lock_file(f)
        try:
            f.seek(0)
            f.write(content)
            f.truncate()
        finally:
            _unlock_file(f)


def add_staff(path: Path, email: str) -> tuple[bool, str]:
    """Add a staff email. Returns (success, message)."""
    email = email.strip().lower()
    if not EMAIL_RE.match(email):
        return False, f"Invalid email format: {email}"

    current = read_staff(path)
    if email in [e.lower() for e in current]:
        return False, f"Already exists: {email}"

    current.append(email)
    _write_staff(path, current)
    logger.info("Added staff: %s", email)
    return True, f"Added: {email}"


def remove_staff(path: Path, email: str) -> tuple[bool, str]:
    """Remove a staff email. Returns (success, message)."""
    email = email.strip().lower()
    current = read_staff(path)
    lower_map = {e.lower(): e for e in current}

    if email not in lower_map:
        return False, f"Not found: {email}"

    current.remove(lower_map[email])
    _write_staff(path, current)
    logger.info("Removed staff: %s", email)
    return True, f"Removed: {email}"
