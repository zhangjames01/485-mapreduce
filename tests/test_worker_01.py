"""See unit test function docstring."""

import json
import socket
import mapreduce
import utils


def manager_message_generator(mock_socket):
    """Fake Manager messages."""
    # First message
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode('utf-8')
    yield None

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_register(mocker):
    """Verify worker registers with manager.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    # Mock socket library functions to return sequence of hardcoded values
    mockclientsocket = mocker.MagicMock()
    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket = mocker.patch('socket.socket')
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    mockclientsocket.recv.side_effect = manager_message_generator(mock_socket)

    # Run student worker code.  When student worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
            manager_hb_port=5999,
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify TCP socket configuration
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the worker uses
        # to receive status update JSON messages from the manager.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(('localhost', 6001)),
        mocker.call().__enter__().listen(),
    ], any_order=True)

    # Verify messages sent by the Worker, excluding heartbeat messages
    messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]
