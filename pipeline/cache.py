"""Cache manager with metadata tracking."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


class CacheManager:
    """Manages cached API responses with metadata for staleness tracking."""

    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_paths(self, key: str) -> tuple[Path, Path]:
        """Get data and metadata file paths for a cache key."""
        safe_key = hashlib.sha256(key.encode()).hexdigest()[:16]
        data_path = self.cache_dir / f"{safe_key}.json"
        meta_path = self.cache_dir / f"{safe_key}.meta.json"
        return data_path, meta_path

    def get(self, key: str, max_age_days: int | None = None) -> dict | list | None:
        """
        Retrieve cached data if valid.

        Args:
            key: Cache key (usually URL or identifier)
            max_age_days: Maximum age in days before considering stale

        Returns:
            Cached data or None if missing/stale
        """
        data_path, meta_path = self._get_paths(key)

        if not data_path.exists() or not meta_path.exists():
            return None

        meta = json.loads(meta_path.read_text())
        fetched_at = datetime.fromisoformat(meta["fetched_at"])

        if max_age_days is not None:
            age_days = (datetime.now(timezone.utc) - fetched_at).days
            if age_days > max_age_days:
                return None

        return json.loads(data_path.read_text())

    def set(self, key: str, data: dict | list, source_url: str | None = None) -> None:
        """
        Store data in cache with metadata.

        Args:
            key: Cache key
            data: Data to cache (must be JSON-serializable)
            source_url: Original URL for reference
        """
        data_path, meta_path = self._get_paths(key)

        data_path.write_text(json.dumps(data, indent=2))

        meta = {
            "key": key,
            "source_url": source_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "size_bytes": data_path.stat().st_size,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

    def invalidate(self, key: str) -> bool:
        """Remove cached data for a key. Returns True if data existed."""
        data_path, meta_path = self._get_paths(key)
        existed = data_path.exists()

        if data_path.exists():
            data_path.unlink()
        if meta_path.exists():
            meta_path.unlink()

        return existed

    def get_metadata(self, key: str) -> dict | None:
        """Get metadata for a cached item without loading data."""
        _, meta_path = self._get_paths(key)
        if meta_path.exists():
            return json.loads(meta_path.read_text())
        return None
