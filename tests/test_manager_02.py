"""See unit test function docstring."""

import json
import distutils.dir_util
import logging
import mapreduce
import utils
from utils import TESTDATA_DIR


def worker_message_generator(mock_socket, manager_log):
    """Fake Worker messages."""
    # Worker register
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode('utf-8')
    yield None

    # User submits new job
    yield json.dumps({
        'message_type': 'new_manager_job',
        'input_directory': TESTDATA_DIR/'input',
        'output_directory': "tmp/test_manager_02/output",
        'mapper_executable': TESTDATA_DIR/'exec/wc_map.sh',
        'reducer_executable': TESTDATA_DIR/'exec/wc_reduce.sh',
        'num_mappers': 2,
        'num_reducers': 1
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for Manager to create directories
    utils.wait_for_isdir("tmp/job-0")

    # Simulate files created by Worker
    distutils.dir_util.copy_tree(
        TESTDATA_DIR/"test_manager_02/intermediate/job-0",
        "tmp/job-0"
    )

    # Wait for Manager to send one map message
    utils.wait_for_map_messages(mock_socket, num=1)

    # Status finished message from both mappers
    yield json.dumps({
        "message_type": "finished",
        "task_id": 0,
        "output_paths": [
            "tmp/job-0/intermediate/maptask00000-part00000",
        ],
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode('utf-8')
    yield None

    # Wait for Manager to send one more map message
    utils.wait_for_map_messages(mock_socket, num=2)
    yield json.dumps({
        "message_type": "finished",
        "task_id": 1,
        "output_paths": [
            "tmp/job-0/intermediate/maptask00001-part00000",
        ],
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode('utf-8')
    yield None

    # Wait for "end map stage" logging message
    utils.wait_for_log(manager_log, "end map stage")

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_map(mocker, caplog):
    """Verify content of map messages sent by the manager.

    Note: 'mocker' is a fixture function provided by the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'caplog' is a fixture function provided by the pytest package.
    This fixture provides access to the logs produced during the test.

    See https://docs.pytest.org/en/6.2.x/logging.html#caplog-fixture for more
    info.
    """
    utils.create_and_clean_testdir("test_manager_02")

    # Monitor logging at INFO level
    caplog.set_level(logging.INFO)

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = \
        worker_message_generator(mock_socket, caplog)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.__enter__.return_value.recv.side_effect = \
        utils.worker_heartbeat_generator(("localhost", 3001))

    # Run student manager code.  When student manager calls recv(), it will
    # return the faked responses configured above.
    try:
        mapreduce.manager.Manager("localhost", 6000, 5999)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify first 3 messages sent by the manager
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs tests/test_manager_X.py
    messages = utils.get_messages(mock_socket)
    assert messages[:3] == [
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_map_task",
            "task_id": 0,
            "executable": str(TESTDATA_DIR/"exec/wc_map.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file01"),
                str(TESTDATA_DIR/"input/file03"),
                str(TESTDATA_DIR/"input/file05"),
                str(TESTDATA_DIR/"input/file07"),
            ],
            "output_directory": "tmp/job-0/intermediate",
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_map_task",
            "task_id": 1,
            "executable": str(TESTDATA_DIR/"exec/wc_map.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": "tmp/job-0/intermediate",
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        }
    ]
