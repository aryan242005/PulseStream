from app.database.store import owns_fence


def test_current_owner_can_commit():
    assert owns_fence("worker-a", 9, "worker-a", 9)


def test_stale_owner_or_token_is_rejected():
    assert not owns_fence("worker-b", 10, "worker-a", 9)
    assert not owns_fence("worker-a", 10, "worker-a", 9)
