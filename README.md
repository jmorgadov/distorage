# Distorage

A distributed storage system implemented in python.

## Table of content
- [Quick Start](https://github.com/jmorgadov/distorage#quick-start)
  - [Start a server](https://github.com/jmorgadov/distorage#start-a-server)
  - [Connect a client](https://github.com/jmorgadov/distorage#connect-a-client)
- [Docker workflow](https://github.com/jmorgadov/distorage#docker-workflow)
  - [Build docker image](https://github.com/jmorgadov/distorage#build-docker-image)
  - [Run server](https://github.com/jmorgadov/distorage#run-server)
  - [Run several servers at once](https://github.com/jmorgadov/distorage#run-several-servers-at-once)

## Quick start

## Start a server

```bash
python main.py server [COMMAND] [ARGS]
```

**Server commands are:**

- `new [PASSWORD]`

   Starts a new service.

- `discover [PASSWORD]`

  Look for other servers in the local network.

- `connect [IP_ADDR] [PASSWORD]`

  Connect to a server in a specified IP address.

> The `[PASSWORD]` argument can be omited, in that case it will be asked later
> in the standar input.

### Conect a client

> Client side is on progress 🚧

## Docker workflow

### Build docker image

```bash
docker build --tag distorage .
```

> ⚠️ It is important to use this tag for the following to work.

### Run a server

```bash
./docker_scripts/run_single_server [COMMAND] [ARGS]
```

### Run several servers at once

```bash
./docker_scripts/run_servers [COUNT] [PASSWORD]
```

Where `[COUNT]` is the number of servers to start.

> With this option the first server will be run with the `new` command to start
> a new service and the rest will be started with the `discover` command.
