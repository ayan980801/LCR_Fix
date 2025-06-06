import os
import sys
import pytest
from unittest import mock

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Provide fake secrets for the ingest module
os.environ.setdefault("KEY-VAULT-SECRET_DATAPRODUCT-SF-EDW-USER", "user")
os.environ.setdefault("KEY-VAULT-SECRET_DATAPRODUCT-SF-EDW-PASS", "pass")

import ingest


def test_truncate_called_for_all_tables():
    called = []

    def _mock_truncate(name):
        called.append(name)

    with mock.patch.object(ingest, "truncate_table", side_effect=_mock_truncate), \
         mock.patch.object(ingest, "load_raw_data", side_effect=RuntimeError("stop")):
        for tbl in ingest.tables:
            with pytest.raises(RuntimeError):
                ingest.process_table(tbl, write_mode="append", historical_load=True)

    assert set(called) == set(ingest.tables)
