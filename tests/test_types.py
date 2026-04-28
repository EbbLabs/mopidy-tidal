import pytest

from mopidy_tidal import types


class TestLazy:
    def test_raises_if_not_set(self):
        with pytest.raises(ValueError):
            types.Lazy().get()

    def test_value_retrievable_after_setting(self):
        lazy = types.Lazy()

        lazy.set(123)

        assert lazy.get() == lazy.get() == 123

    def test_callback_used_when_value_missing(self):
        called = 0

        def init() -> int:
            nonlocal called
            called += 1
            return 123

        lazy = types.Lazy()

        assert lazy.get_or(init) == 123
        assert lazy.get_or(init) == 123
        assert called == 1

    def test_get_or_none_elides_inner_optionals(self):
        assert types.Lazy(inner=None).get_or_none() is None
