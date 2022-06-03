# This is just to declare some basic needed actions
# that a server will do. This might be inplemented
# in other modules in the future.
"""
This will contain all the server related implementation of the system.
"""

def discover_servers():
    raise NotImplementedError()


def connect_to_sistem():
    raise NotImplementedError()


def wait_for_clients():
    raise NotImplementedError()


def attend_client():
    raise NotImplementedError()


def wait_for_other_servers():
    raise NotImplementedError()


def main():
    discover_servers()
    connect_to_sistem()
    wait_for_clients()
    wait_for_other_servers()


if __name__ == "__main__":
    main()
