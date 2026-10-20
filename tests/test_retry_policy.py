from app.worker.policy import retry_delay, should_dead_letter


def test_retry_delay_grows_and_is_capped():
    assert [retry_delay(n) for n in range(1, 5)] == [1, 2, 4, 8]
    assert retry_delay(99) == 30


def test_dead_letter_limit_is_bounded():
    assert should_dead_letter(5)
    assert not should_dead_letter(4)
