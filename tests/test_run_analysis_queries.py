from unittest.mock import patch

import pytest

import scripts.run_analysis_queries as runner


class _DummyConn:
    pass


class _DummyTxn:
    def __enter__(self):
        return _DummyConn()

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyEngine:
    def begin(self):
        return _DummyTxn()


def test_analysis_runner_exits_non_zero_when_any_query_fails():
    failing_query = runner.QUERY_FILES[1]

    def fake_run_file(conn, path, output_dir):
        if path.name == failing_query:
            raise RuntimeError("simulated query failure")

    with patch.object(runner, "create_engine", return_value=_DummyEngine()), patch.object(
        runner, "run_file", side_effect=fake_run_file
    ):
        with pytest.raises(SystemExit) as exc:
            runner.main()

    assert exc.value.code == 1
