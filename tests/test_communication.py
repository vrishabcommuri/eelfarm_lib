# Author: Christian Brodbeck <christianbrodbeck@nyu.edu>
from socket import gethostname

from eelbrain.load import unpickle
import eelfarm
from eelfarm._worker import WorkerClient
import pytest
from time import sleep


@pytest.fixture()
def worker_and_server():
    server = eelfarm.start_server()
    worker = WorkerClient(server.host, [server.host], sleep_time=0.001)
    worker.start()
    yield worker, server
    worker.terminate(block=True)
    server.shutdown(block=True)


def test_communication(tmp_path, worker_and_server):
    worker, server = worker_and_server
    dst = tmp_path / 'product.pickle'
    server.put(dst, list, range(3))

    result_path = server.get(block=True, timeout=5)
    assert result_path == str(dst)
    result = unpickle(result_path)
    assert result == [0, 1, 2]

    host = gethostname()
    server._send(host, 'info')

    server._send(host, 'client', f'2 {server.host}')
    sleep(0.050)
    assert worker.serve == [host, host]

    server._send(host, 'client', server.host)
    sleep(0.050)
    assert worker.serve == [host]

    # job after messages
    dst = tmp_path / 'product_2.pickle'
    server.put(dst, dict, [(1, 'a'), (2, 'b')])
    result_path = server.get(block=True, timeout=5)
    assert result_path == str(dst)
    result = unpickle(result_path)
    assert result == {1: 'a', 2: 'b'}
