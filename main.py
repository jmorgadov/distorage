import sys

from distorage.server import app

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Please specify what to run: server or client")
    if sys.argv[1] == "server":
        sys.argv.pop(1)
        app.main()
    elif sys.argv[1] == "client":
        raise NotImplementedError("Client is not implemented yet")
