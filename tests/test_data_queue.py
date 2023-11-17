import json
import pytest
from pprint import pprint
from py_boilingdata.data_queue import DataQueue


def cb(event):
    print(f"\n\nWe got all the data for requestId '{event['requestId']}'")
    pprint(event["data"])


def add_test_msg(
    q,
    requestId,
    batchSerial,
    totalBatches,
    subBatchSerial,
    totalSubBatches,
    splitSerial,
    totalSplitSerials,
):
    msg = {
        "requestId": requestId,
        "batchSerial": batchSerial,
        "totalBatches": totalBatches,
    }
    if subBatchSerial is not None and totalSubBatches is not None:
        msg["subBatchSerial"] = subBatchSerial
        msg["totalSubBatches"] = totalSubBatches
    if splitSerial is not None and totalSplitSerials is not None:
        msg["splitSerial"] = splitSerial
        msg["totalSplitSerials"] = totalSplitSerials
    # Only one row per message in this test..
    # And we put the serials into the data so we can see whether the output is ordered properly :)
    msg["data"] = (
        [
            {
                "requestId": requestId,
                "batchSerial": batchSerial,
                "totalBatches": totalBatches,
                "subBatchSerial": subBatchSerial,
                "splitSerial": splitSerial,
                "totalSplitSerials": totalSplitSerials,
            }
        ],
    )
    q.push(json.loads(json.dumps(msg)))


def test_local_query_batches_only():
    q = DataQueue("test1a", cb)
    add_test_msg(q, "test1a", 1, 1, None, None, None, None)
    assert q.is_done() == True
    assert q.is_done() == True
    q.delete()
    q = DataQueue("test1b", cb)
    add_test_msg(q, "test1b", 1, 3, None, None, None, None)
    # We don't expect the serials to be consecutive numbers, just different
    add_test_msg(q, "test1b", 123, 3, None, None, None, None)
    assert q.is_done() == False
    assert q.is_done() == False
    # assert q.get_data() == None
    add_test_msg(q, "test1b", 56, 3, None, None, None, None)
    assert q.is_done() == True
    assert q.is_done() == True
    # assert q.get_data() == []
    q.delete()


def test_local_query_with_subbatches():
    q = DataQueue("test2", cb)
    add_test_msg(q, "test2", 1, 1, 2, 2, None, None)
    assert q.is_done() == False
    assert q.is_done() == False
    add_test_msg(q, "test2", 1, 1, 1, 2, None, None)
    assert q.is_done() == True
    assert q.is_done() == True
    q.delete()


def test_local_query_with_subbatches_and_splits():
    q = DataQueue("test3", cb)
    add_test_msg(q, "test3", 1, 1, 2, 2, 3, 3)
    assert q.is_done() == False
    assert q.is_done() == False
    add_test_msg(q, "test3", 1, 1, 1, 2, 1, 3)
    assert q.is_done() == False
    assert q.is_done() == False
    add_test_msg(q, "test3", 1, 1, 2, 2, 1, 3)
    add_test_msg(q, "test3", 1, 1, 2, 2, 2, 3)
    assert q.is_done() == False
    assert q.is_done() == False
    add_test_msg(q, "test3", 1, 1, 2, 2, 3, 3)
    assert q.is_done() == False
    assert q.is_done() == False
    add_test_msg(q, "test3", 1, 1, 1, 2, 2, 3)
    add_test_msg(q, "test3", 1, 1, 1, 2, 3, 3)
    assert q.is_done() == True
    assert q.is_done() == True
    assert list(
        map(
            lambda x: f"{x[0]['batchSerial']}{x[0]['subBatchSerial']}{x[0]['splitSerial']}",
            q.get_data(),
        )
    ) == ["111", "112", "113", "121", "122", "123"]
    q.delete()
