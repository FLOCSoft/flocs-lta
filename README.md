# flocs-lta
A package that focuses on interaction with the LOFAR Long Term Archive within the FLOCS ecosystem.

## Installation

Flocs-lta has a few non-standard dependencies to interact with the LTA. To use flocs-lta, do **not** follow the LTA client instructions as given in the readme of lofar_lta. They are outdated and not applicable for at least Python 3.12 and up. Instead, follow the instructions here. The following dependencies are needed:

* lofar_stager_api: https://git.astron.nl/astron-sdc/lofar_stager_api
* lofar_lta: https://lta.lofar.eu/software/
* Oracle instant client

### Installing lofar_stager_api

Clone the lofar_stager_api from https://git.astron.nl/astron-sdc/lofar_stager_api and add it to your `PYTHONPATH`.

### Installing lofar_lta

Download LTA 2.8.0 (or newer) from https://lta.lofar.eu/software/. Unzip and run

```
uv pip install oracledb
uv pip install lofar_lta-2.8.0
```

### Installing Oracle Instant Client

Go to https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html and download the basic package. Unzip, and add the resulting folder (e.g. `instantclient_23_26`) to `LD_LIBRARY_PATH`.

## Setting up the enviornment for interacting with the LTA

To use flocs-lta, we need set up interaction with the StageIt service and with the LTA database. First make a file under `$HOME/.stagingrc` with your username and StageIt token. This will be used for downloading data. Next, configure your AWE environment by editing `$HOME/.awe/Environment.cfg`. Set `database_user` and `database_password` to your LTA credenditals, and add an entry for `database_api` set to `oracledb-thin` (or modify it if it h as been defined already).

## Installing flocs-lta

The FLOCS LTA package can be installed as usual with pip:

```bash
uv pip install git+https://github.com/FLOCSoft/flocs-lta.git
```

If everything went well, you should be able to see the help via `flocs-lta --help`.
