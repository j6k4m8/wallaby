from . import Wallaby
import time

class TestWallaby:

    def test_can_create(self):
        w = Wallaby('file::memory:?cache=shared')
        assert w._sqlite_database_path == 'file::memory:?cache=shared'

    def test_can_log_string_results(self):
        w = Wallaby('file::memory:?cache=shared')
        assert w.log("This is my result") == True

    def test_can_log_json_results_and_return_pandas(self):
        w = Wallaby('file::memory:?cache=shared')
        assert w.log({
            "favorite_number": 42
        }) == True
        assert "42" in w.get_results_since(0, as_dataframe=True).results.iloc[0]

    def test_can_get_by_tag(self):
        w = Wallaby('file::memory:?cache=shared')
        assert w.log({
            "favorite_number": 42
        }, tags=["foo", "bar"]) == True
        assert len(w.get_by_tag(all_of=["ba"])) == 0
        assert len(w.get_by_tag(any_of=["foo"])) == 1
        assert len(w.get_by_tag(all_of=["foo"])) == 1
        assert len(w.get_by_tag(all_of=["foob"])) == 0
        assert len(w.get_by_tag(any_of=["baz"])) == 0
        assert len(w.get_by_tag(all_of=["foo", "bar"])) == 1
