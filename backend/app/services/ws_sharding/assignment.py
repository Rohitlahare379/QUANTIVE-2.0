"""
Deterministic WebSocket Shard Assignment Module.

Algorithm:
1. Symbol Normalization:
   - Strip leading/trailing whitespace: symbol.strip()
   - Convert to uppercase: symbol.upper()
   - Reject empty strings.
2. Deterministic Hashing:
   - Compute SHA-256 digest of the UTF-8 encoded normalized symbol string.
   - Extract the first 8 bytes as an unsigned 64-bit big-endian integer.
   - Compute modulo `num_shards`: `(hash_uint64 % num_shards)` -> integer in [0, num_shards - 1].
3. Properties:
   - Pure functional and stateless (no database queries or network lookups needed).
   - Independent of Python process memory layout or `PYTHONHASHSEED` randomization.
   - Uniform distribution across shards.
"""

import hashlib
from typing import Dict, Iterable, List, Optional
from app.core.config import settings


def normalize_symbol(symbol: str) -> str:
    """
    Normalizes a ticker symbol by trimming whitespace and converting to uppercase.
    
    Raises:
        ValueError: If the normalized symbol is empty.
    """
    if not isinstance(symbol, str):
        raise TypeError(f"Symbol must be a string, got {type(symbol).__name__}")
    
    clean = symbol.strip().upper()
    if not clean:
        raise ValueError("Symbol cannot be empty or blank")
    return clean


def get_shard_for_symbol(symbol: str, num_shards: Optional[int] = None) -> int:
    """
    Deterministically computes the shard ID (0 to num_shards - 1) for a given symbol.

    Args:
        symbol: The asset symbol (e.g., 'BTCUSDT', 'ethusdt', ' SOLUSDT ').
        num_shards: Total number of shards. Defaults to settings.WS_NUM_SHARDS.

    Returns:
        int: Deterministic shard index in [0, num_shards - 1].

    Raises:
        ValueError: If num_shards <= 0 or symbol is empty.
    """
    if num_shards is None:
        num_shards = settings.WS_NUM_SHARDS

    if num_shards <= 0:
        raise ValueError(f"num_shards must be a positive integer, got {num_shards}")

    clean_symbol = normalize_symbol(symbol)
    
    # Deterministic 64-bit integer from SHA-256 digest
    digest = hashlib.sha256(clean_symbol.encode("utf-8")).digest()
    hash_int = int.from_bytes(digest[:8], byteorder="big", signed=False)
    
    return hash_int % num_shards


def assign_symbols_to_shards(
    symbols: Iterable[str],
    num_shards: Optional[int] = None
) -> Dict[int, List[str]]:
    """
    Partitions a collection of symbols into a dictionary mapping shard_id to a sorted list of symbols.

    Args:
        symbols: Iterable of symbol strings.
        num_shards: Total number of shards. Defaults to settings.WS_NUM_SHARDS.

    Returns:
        Dict[int, List[str]]: Mapping from shard_id (0..num_shards-1) to sorted unique normalized symbols.
    """
    if num_shards is None:
        num_shards = settings.WS_NUM_SHARDS

    if num_shards <= 0:
        raise ValueError(f"num_shards must be a positive integer, got {num_shards}")

    buckets: Dict[int, set] = {i: set() for i in range(num_shards)}
    for sym in symbols:
        clean = normalize_symbol(sym)
        shard_id = get_shard_for_symbol(clean, num_shards)
        buckets[shard_id].add(clean)

    return {shard_id: sorted(list(sym_set)) for shard_id, sym_set in buckets.items()}


def get_symbols_for_shard(
    symbols: Iterable[str],
    shard_id: int,
    num_shards: Optional[int] = None
) -> List[str]:
    """
    Filters and returns only the normalized symbols assigned to the specified shard_id.

    Args:
        symbols: Iterable of symbol strings.
        shard_id: Target shard ID to filter by.
        num_shards: Total number of shards. Defaults to settings.WS_NUM_SHARDS.

    Returns:
        List[str]: Sorted list of unique normalized symbols belonging to shard_id.

    Raises:
        ValueError: If shard_id is out of range [0, num_shards - 1].
    """
    if num_shards is None:
        num_shards = settings.WS_NUM_SHARDS

    if shard_id < 0 or shard_id >= num_shards:
        raise ValueError(f"shard_id {shard_id} is out of valid range [0, {num_shards - 1}]")

    result = set()
    for sym in symbols:
        clean = normalize_symbol(sym)
        if get_shard_for_symbol(clean, num_shards) == shard_id:
            result.add(clean)

    return sorted(list(result))
