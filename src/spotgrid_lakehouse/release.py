"""Publish files as a GitHub release, replacing same-named assets on a rerun."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import httpx

API = "https://api.github.com"


def publish(repo: str, token: str, tag: str, title: str, notes: str, files: list[Path]) -> str:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    with httpx.Client(headers=headers, timeout=120) as http:
        found = http.get(f"{API}/repos/{repo}/releases/tags/{tag}")
        if found.status_code == 404:
            created = http.post(
                f"{API}/repos/{repo}/releases", json={"tag_name": tag, "name": title, "body": notes}
            )
            created.raise_for_status()
            release = created.json()
        else:
            found.raise_for_status()
            release = found.json()
        existing = {a["name"]: a["id"] for a in release.get("assets", [])}
        for file in files:
            if file.name in existing:
                http.delete(
                    f"{API}/repos/{repo}/releases/assets/{existing[file.name]}"
                ).raise_for_status()
            upload = (
                f"https://uploads.github.com/repos/{repo}/releases/{release['id']}/assets"
                f"?name={quote(file.name)}"
            )
            http.post(
                upload,
                content=file.read_bytes(),
                headers={"Content-Type": "application/octet-stream"},
            ).raise_for_status()
        return release["html_url"]
