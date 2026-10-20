import zlib


def shard_for(key: str, count: int = 3) -> int:
    return zlib.crc32(key.encode()) % count


def test_partition_is_deterministic_and_bounded():
    assert shard_for("account-17") == shard_for("account-17")
    assert all(0 <= shard_for(str(i)) < 3 for i in range(500))


def test_shard_count_partitions_sample_keys():
    assert len({shard_for(f"key-{i}") for i in range(100)}) == 3
