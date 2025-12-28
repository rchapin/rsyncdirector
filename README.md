# RsyncDirector

Automate and coordinate the running of `rsync` jobs on multiple Linux hosts with a `cron` syntax schedule.

## Overview

`rsyncdirector` enables multiple `rsync` jobs, running locally or remotely, to synchronize on lock files, local or remote, to ensure that data is only mutated by one `rsync` process at a time.

For example:  Every 24 hours `rsyncdirector` runs on `laptop01` backing up data to a `backup01`.  Both machines are in the same physical location.  Every 7 days an instance of `rsyncdirector` on `backup01` `rsync`s all of its data to `backup-remote` which is at a remote site.  Before the instance of `rsyncdirector` on `laptop01` starts syncing data to `backup01` it checks to see if a lockfile exists in a predefined location on `backup01`.  If it exists, it indicates that `backup01` is currently syncing data and the `laptop01` `rsyncdirector` waits a configured amount of time and then re-checks, until the lock file no longer exists, or a configured timeout exipres.  Once the `laptop01` `rsyncdirector` sees that the lockfile on `backup01` is gone, it writes a lockfile to `backup01` and starts `rsync`ing data from itself to `backup01`.  In this way, multiple `rsyncdirector` instances can be run across multiple hosts and coordinate so that data is not mutated at the same time.

`rsyncdirector` runs as a `systemd` service via a template unit file to enable running multiple `rsyncdirector` instances on a single host.

`rsyncdirector` communicates to remote hosts over SSH using the Python [Fabric](https://www.fabfile.org/) library which depends on the Python [Paramiko](https://www.paramiko.org/) library for the core SSH protocol implementation.  SSH connections are currently authenticated using passphrase-less SSH keys.

`rsync` functionality is implemented by the underlying `rsync` package installed on the host.

Each instance can be configured to run under any arbitrary user and connect to any remote host that it can reach with the supplied credentials.  The user under which it runs must have read permissions for the files that it is configured to `rsync`.  The administrator must distribute public SSH keys to the hosts to which data is to be `rsync`ed and can optionally specify a private key other than the `rsyncdirector` user's default set of private keys.

See the annotated, [example config file](rsyncdirector/resources/rsyncdirector.yaml) for details on how to configure an instance of `rsyncdirector`.

There is a companion program, [`rsyncdirector_deploy`](https://github.com/rchapin/rsyncdirector_deploy), to assist in the installation and deployment of configurations of `rsyncdirector`.

## Forcing a Run Now

The `rsyncdirector` listens for `SIGHUP` events and when receives one will immediately schedule a `run-once` execution of the configured jobs.

1. Get the PID of the `rsyncdirector` process.  If there are multiple instances of it running you will need to adjust the way you search for the PID.
    ```
    pgrep rsyncdirector
    ```

1. Send the `SIGHUP`
    ```
    kill -SIGHUP <PID>
    ```

## Key Concepts

Much of the following concepts map to specific configuration parameters.

### cron_schedule
Each `rsyncdirector` instance requires defining a peridocity for which it will run by defining a cron expression in the config which uses the same syntax as a standard Linux cron job.

### job
Each job encapsulates n number of `rsync` commands that either sync data between directories on the localhost or sync data to a single remote host.  The job defines the specifics for connecting to a remote host if the job type is `remote`.

### syncs
`syncs` define the specific `rsync` command to be run.  Each "inherits" the definitions of the job to enable the concatenation of the `rsync` command to include the specified user, host, and port information for `remote` jobs.  The `source` and `dest` are self-explanatory, and the `opts` list enables the inclusion of any arbitrary options that  the undelying `rsync` implementation on the host allows.

### lock_files
`lock_files` define an arbitrary number of files that will be created on either the localhost or remote host(s) to signal to other `rsyncdirector` instances that a given instance is running.

### blocks_on
`blocks_on` define an arbitrary number of `lock_files` from other `rsyncdirector` instances that the `job` will wait on before continuing with the `job`.

## Metrics

`rsyncdirector` exposes metrics via the `prometheus_client` via `http://$host:$port/metrics`.  By default, it will listen on port `9090` and can be overridden.

## Installing

There is a companion program, [`rsyncdirector_deploy`](https://github.com/rchapin/rsyncdirector_deploy), that contains all of the configuration templates and automation to assist in the installation and deployment of configurations of `rsyncdirector`.

The easiest way to install and configure it is with the aformentioned program.  If you want to install it by hand, you will need to get the configuration templates from the `rsyncdirector_deploy` repo.

## Building, Developing, and Testing

This requires a compatible version of Python in your path from which the path to the interpreter can be gleaned by running `which python<version>`.  See the `requires-python` entry in `pyproject.toml` at the root of the repo for the currently required version of Python.

Once that is installed, run `./run-tests.sh --dev-setup` to create a development virtual environment.

### Building a Distribution

```
python -m build
twine check dist/*
```

#### Uploading to PyPi

1. Generate an API token and add the requisite `[pypi]` entry to the `~/.pypirc` file
    ```
    [pypi]
    username = __token__
    password = <API-token>
    ```

1. Then upload the artifacts to PyPi
    ```
    python -m twine upload --repository pypi dist/*
    ```

#### Uploading to Nexus

In my case, I have an instance of Nexus running in my network with a `~/.pypirc` file configured for it and publish the artifacts there with the following command:
```
twine upload --cert <path-to-nexus-cert>--verbose --repository pypi-[dev|release] dist/rsyncdirector-<version>.tar.gz
```

### Running the Tests

The integration tests require that the user running the tests can ssh to `root@localhost` witout having to enter the password.  To do so add a public ssh key of the user running the tests to the authorized keys of the `root` user on the localhost.

Then, from the root of the repo run
```
./run-tests.sh
```

### Setting Up to Run and Debug Integration Tests in VSCode

The requisite configs are already present in the provided `.vscode/settings.json` file.

1. Export the path to the required version of Python that you want to use.  If you already have one that will suffice, skip this step.
    ```
    export RSYNCDIRECTORINTTEST_PYTHON=<path-to-python-binary>
    ```

1. Setup the test environment.  The following will create a `.env` file at the root of the repository that VSCode will read while running the tests and build the test containers.
    ```
    ./run-tests.sh --dev-setup
    ```

1. Configure VSCode
    1. Press `CTRL+SHIFT+P` and select **Python: Configure Tests**
    1. Select `pytest`.  Even though we are using the `unittest` library for testing this seems to be the only test configuration in VSCode that works for running and debugging the tests for the time being.
    1. Choose **rsyncdirector** as the root directory for the tests
    1. Click on the **Testing** icon in the left-hand side-bar and you should see a list of all of the tests that you can now run or debug.

        > If you do not see any tests listed or see an error in the panel check the **Output** Panel and select **Python** from the dropdown menu for any relevant error messages.

1. Update env vars so that existing test timeouts don't result in Exceptions while you are stepping through the code:  There are a number of env vars written to the `.env` file that determine timeouts for various waits in the test code.  When running the whole test suite, the defaults in the `.env` file are fine.  When stepping through the code in the IDE these timeouts can expire and throw Exceptions when nothing has yet gone wrong and get in your way of debugging the code.  To avoid this, edit the `.env` file and update the following env vars setting to the following suggested values.  If you re-run `run-tests.sh --dev-setup` it will overwrite your changes to the `.env` file.
    1. `RSYNCDIRECTORINTTEST_WAITFOR_TIMEOUT_SECONDS=1000`

### Setting up to Develop in VSCode

1. Install the **Black Formatter** extension from Microsoft.
1. Open a Python file in VS Code.
1. Right-click on the editor to display the context menu.
1. Select **Format Document With....**
1. Select **Configure Default Formatter...** from the drop-down menu.
1. Select **black** as your preferred formatter extension from the list.

### Dependency Management

First-order dependencies for `requirements.txt`, `requirements_test.txt`, are defined in the respective `.in` files.  If you make updates to the first order dependencies you need to "compile" the full dependency list.  First, ensure that `pip-tools` is installed in your dev virtual environment.  `dev` dependendies are just defined in `requirements_dev.txt`.
```
pip install .[dev]
pip install .[test]
```

Then run the following
```
pip-compile -v --no-emit-trusted-host --no-emit-index-url requirements.in
pip-compile -v --no-emit-trusted-host --no-emit-index-url requirements_test.in
```

## Notes

[This is the solution for terminating a multi process/thread in Python](https://cuyu.github.io/python/2016/08/15/Terminate-multiprocess-in-Python-correctly-and-gracefully).  It was the solution for being able to kill a running rsync job.  Actually run the rsync command in a separate process that I can then kill if we receive a SIGTERM for the parent process.
