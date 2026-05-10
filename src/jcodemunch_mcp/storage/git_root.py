"""Git-root detection for index identity.

Walks up from a path looking for a `.git` directory; when found, derives
the repo identity from `git remote get-url origin` (so a clone of
`elastic/kibana` indexes as `elastic/kibana` regardless of the local
folder name) and falls back to the git-root basename for repos with no
configured remote.

Foundation for #288 — v1.95.0 uses this for identity only; the merge
logic that lets `index ./packages` and `index ./scripts` coalesce into
one `elastic/kibana` index lands in v1.96.
"""

import logging
import re
import subprocess
from pathlib import Path
from typing import NamedTuple, Optional

logger = logging.getLogger(__name__)


class GitRootIdentity(NamedTuple):
    """Result of git-root detection.

    Attributes:
        git_root: Absolute path of the enclosing git working tree.
        owner: Repo owner ("local" if no remote is configured).
        name: Repo name (from `origin` URL or git-root basename).
    """
    git_root: str
    owner: str
    name: str


# git@github.com:owner/repo.git   |   https://github.com/owner/repo(.git)
# Also covers gitlab, bitbucket, and generic git hosts — we just want
# the trailing two path segments.
_REMOTE_OWNER_REPO = re.compile(
    r"""(?:[:/])(?P<owner>[^/:]+)/(?P<name>[^/]+?)(?:\.git)?/?\s*$"""
)


def _find_git_root(start: Path) -> Optional[Path]:
    """Walk up from `start` looking for a `.git` directory or file.

    Returns the absolute path of the enclosing working tree, or None if
    no `.git` is found anywhere up to the filesystem root. Handles
    `.git` as a file (worktrees, submodules) the same as a directory —
    its presence still marks a working tree we should anchor to.
    """
    p = start.resolve()
    if not p.exists():
        return None
    if p.is_file():
        p = p.parent
    for candidate in (p, *p.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _read_origin_url(git_root: Path) -> Optional[str]:
    """Return the `origin` remote URL for a git working tree, or None."""
    try:
        result = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=str(git_root),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        logger.debug("git config probe failed for %s", git_root, exc_info=True)
        return None
    if result.returncode != 0:
        return None
    url = result.stdout.strip()
    return url or None


def _parse_owner_repo(remote_url: str) -> Optional[tuple[str, str]]:
    """Extract `(owner, name)` from a git remote URL.

    Returns None when the URL does not contain a recognizable
    owner/name suffix (e.g. a bare server-relative path).
    """
    m = _REMOTE_OWNER_REPO.search(remote_url)
    if not m:
        return None
    owner = m.group("owner").strip()
    name = m.group("name").strip()
    if not owner or not name:
        return None
    return owner, name


def detect_git_root(path: str) -> Optional[GitRootIdentity]:
    """Detect the enclosing git root and derive a repo identity.

    Resolution order:

    1. No `.git` found anywhere up the tree -> return None.  Caller
       falls back to the basename-keyed identity (today's behavior).
    2. `.git` found and `origin` remote URL parses to `<owner>/<name>`
       -> return that identity.  This makes a clone of
       `https://github.com/elastic/kibana` index as `elastic/kibana`
       regardless of the local folder name.
    3. `.git` found but no usable origin -> return identity
       `("local", <git-root-basename>)`.  Caller may append a
       path-derived hash for stable disambiguation.

    The returned `git_root` is always the absolute path of the working
    tree, suitable for storing on the index manifest as the canonical
    repo location.
    """
    root = _find_git_root(Path(path).expanduser())
    if root is None:
        return None

    url = _read_origin_url(root)
    if url:
        parsed = _parse_owner_repo(url)
        if parsed:
            owner, name = parsed
            return GitRootIdentity(git_root=str(root), owner=owner, name=name)

    return GitRootIdentity(git_root=str(root), owner="local", name=root.name)
