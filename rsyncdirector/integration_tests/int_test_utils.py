# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

from collections import namedtuple
from dataclasses import dataclass, field
from dataclass_wizard import JSONWizard, YAMLWizard
from datetime import datetime, timedelta
from enum import Enum, auto
from fabric import Connection
from typing import List, Optional
import docker
import logging
import os
import random
import string
import sys
import time
import yaml
from rsyncdirector.lib.envvars import EnvVars
from rsyncdirector.lib.utils import Utils
from rsyncdirector.lib.config import JobType


logging.basicConfig(
    format="%(asctime)s,%(levelname)s,%(module)s,%(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


class ContainerType(Enum):
    TARGET = auto()
    REMOTE = auto()


@dataclass
class RemoteHost(JSONWizard):
    user: str
    host: str
    port: str
    private_key_path: str


@dataclass
class LockFile(JSONWizard):
    type: str
    path: str


@dataclass
class RemoteLockFile(LockFile, RemoteHost):
    pass


@dataclass
class BlocksOn(JSONWizard):
    class _(JSONWizard.Meta):
        key_transform_with_dump = "SNAKE"

    type: str
    path: str
    wait_time: int
    timeout: Optional[int] = None


@dataclass
class BlocksOnRemote(BlocksOn, RemoteHost):
    pass


@dataclass
class Action(JSONWizard):
    class _(JSONWizard.Meta):
        key_transform_with_dump = "SNAKE"

    action: str


@dataclass
class SyncAction(Action):
    source: str
    dest: str
    opts: List[str]


@dataclass
class CommandAction(Action):
    command: string
    args: List[str]


@dataclass
class Sync(JSONWizard):
    source: str
    dest: str
    opts: List[str]


@dataclass
class Job(JSONWizard):
    class _(JSONWizard.Meta):
        key_transform_with_dump = "SNAKE"

    id: str
    type: str
    lock_files: List[LockFile]
    blocks_on: List[BlocksOn]
    actions: List[Action]


@dataclass
class JobRemote(Job, RemoteHost):
    pass


@dataclass
class Metrics(JSONWizard):
    class _(JSONWizard.Meta):
        key_transform_with_dump = "SNAKE"

    addr: Optional[str] = None
    port: Optional[str] = None


@dataclass
class AppConfigs(JSONWizard):
    class _(JSONWizard.Meta):
        key_transform_with_dump = "SNAKE"

    rsync_id: str
    cron_schedule: str
    pid_file_dir: str
    jobs: List[Job]
    metrics: Optional[Metrics] = None


TEST_CONF_NAMED_TUPLE = "TestConfigs"
ENV_VAR_PREFIX = "RSYNCDIRECTORINTTEST"
WAIT_FOR_DOCKER_SSH_SLEEP_TIME = 1
WAIT_FOR_DOCKER_SHUTDOWN_TIME = 2
RSYNC_ID_DEFAULT = "test_rsync"


class IntegrationTestUtils(object):

    @staticmethod
    def build_base_config(configs):
        retval = {}
        retval["pid_file_dir"] = configs.pid_dir
        return retval

    @staticmethod
    def create_test_file(output_dir, file_name, num_chars) -> int:
        """
        Will create a test file with the specified number of random characters.
        """
        chunk_size = 1024 * 1024
        # Create the dir if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, file_name)
        with open(output_path, "w") as fh:
            chars_written = 0
            chars_remaining = num_chars
            while chars_written < num_chars:
                chars_to_write = chunk_size if chars_remaining > chunk_size else chars_remaining
                data = "".join(
                    random.choices(string.ascii_uppercase + string.digits, k=chars_to_write)
                )
                fh.write(data)
                chars_written = chars_written + chars_to_write
                chars_remaining = chars_remaining - chars_written

        size = os.path.getsize(output_path)
        return size

    @staticmethod
    def get_test_docker_conn(test_configs, container_type) -> Connection:
        port = None
        match container_type:
            case ContainerType.TARGET:
                port = test_configs.container_target_port
            case ContainerType.REMOTE:
                port = test_configs.container_remote_port
            case _:
                raise ValueError(f"Invalid container type: {container_type}")

        return Connection(
            host=test_configs.test_host,
            user="root",
            port=port,
            connect_kwargs=dict(key_filename=test_configs.ssh_identity_file),
        )

    @staticmethod
    def is_docker_container_running(container_name, docker_client):
        container = None
        is_running = False

        containers = docker_client.containers.list(filters={"name": container_name})
        if len(containers) > 0:
            container = containers[0]
            if container.status == "running":
                is_running = True

        return container, is_running

    @staticmethod
    def get_app_configs(
        test_configs: namedtuple, job_type: JobType, rsync_id: str = None
    ) -> AppConfigs:
        retval = None

        match job_type:
            case JobType.LOCAL:
                retval = AppConfigs(
                    rsync_id=rsync_id if rsync_id is not None else RSYNC_ID_DEFAULT,
                    cron_schedule="* * * * *",
                    pid_file_dir=test_configs.pid_dir,
                    jobs=[
                        Job(
                            id="local_to_local",
                            type="local",
                            lock_files=[
                                LockFile(
                                    type="local",
                                    path=os.path.join(test_configs.lock_dir, "lock_file"),
                                ),
                            ],
                            actions=None,
                            blocks_on=None,
                        )
                    ],
                )
            case JobType.REMOTE:
                retval = AppConfigs(
                    rsync_id=rsync_id if rsync_id is not None else RSYNC_ID_DEFAULT,
                    cron_schedule="* * * * *",
                    pid_file_dir=test_configs.pid_dir,
                    jobs=[
                        JobRemote(
                            id="local_to_container",
                            type="remote",
                            user="root",
                            host=test_configs.test_host,
                            port=test_configs.container_target_port,
                            private_key_path=test_configs.ssh_identity_file,
                            lock_files=[
                                LockFile(
                                    type="local",
                                    path=os.path.join(test_configs.lock_dir, "lock_file"),
                                ),
                            ],
                            actions=None,
                            blocks_on=None,
                        )
                    ],
                )
            case _:
                self.fail(f"unknown JobType; job_type={job_type}")

        # Always add overriding metrics configs for the listening port so that we do not collide
        # with an instance that might be running on the deveoper workstation.
        retval.metrics = Metrics(
            port=test_configs.metrics_scraper_target_port,
        )

        return retval

    @classmethod
    def get_test_configs(cls):
        """
        Dynamically builds a named tuple from the env vars exported that are prefixed by the
        ENV_VAR_PREFIX string.
        """
        env_vars = EnvVars.get_env_vars(ENV_VAR_PREFIX)
        attributes_list = []
        values = []
        for k, v in env_vars.items():
            """
            Generate an attribute name for the namedtuple by removing the
            the prefix from the key and converting it to lower-case.
            """
            key = k.replace(f"{ENV_VAR_PREFIX}_", "").lower()
            attributes_list.append(key)

            # We know that there are some configs that have to be parsed into numerical values and we
            # will just do it here so that we don't have to do it multiple times in the test code.
            parse_to_float_keys = ["waitfor_timeout_seconds", "waitfor_poll_interval"]
            if key in parse_to_float_keys:
                v = float(v)

            values.append(v)

        attributes = " ".join(attributes_list)
        test_conf = namedtuple(TEST_CONF_NAMED_TUPLE, attributes)
        retval = test_conf(*values)

        # Generate a log message of all of the test config values.
        entries = []
        idx = 0
        for attrib in attributes_list:
            entries.append(f"{attrib}:{values[idx]}")
            idx += 1

        entries.sort()
        log_msg = "\n".join(entries)
        logger.info(f"Generating TestConfigs namedtuple with attributes:\n{log_msg}")
        return retval

    @staticmethod
    def restart_docker_containers(configs):
        client = docker.from_env()
        IntegrationTestUtils.stop_docker_containers(configs, client)
        IntegrationTestUtils.start_docker_containers(configs, client)
        client.close()

    @staticmethod
    def remove_nulls_from_dict(d):
        retval = {}
        for k, v in d.items():
            if isinstance(v, dict):
                sub_dict = IntegrationTestUtils.remove_nulls_from_dict(v)
                if sub_dict is not None:
                    retval[k] = sub_dict
            elif isinstance(v, list):
                clean_list = []
                for e in v:
                    if isinstance(e, dict):
                        sub_dict = IntegrationTestUtils.remove_nulls_from_dict(e)
                        if sub_dict is not None:
                            clean_list.append(sub_dict)
                    elif e is not None:
                        clean_list.append(e)
                if len(clean_list) > 0:
                    retval[k] = clean_list
            elif v is not None:
                retval[k] = v

        return retval

    @staticmethod
    def set_env_vars(env_vars: dict) -> None:
        for k, v in env_vars.items():
            os.environ[k] = v

    @staticmethod
    def unset_env_vars(env_vars: dict) -> None:
        for k in env_vars.keys():
            os.environ.pop(k)

    @staticmethod
    def start_docker_containers(configs, docker_client):
        containers = [
            (configs.container_target_name, configs.container_target_port),
            (configs.container_remote_name, configs.container_remote_port),
        ]
        for name, port in containers:
            p = {"22/tcp": ("0.0.0.0", port)}
            container = docker_client.containers.run(
                configs.image_name,
                detach=True,
                ports=p,
                name=name,
                auto_remove=True,
            )

            IntegrationTestUtils.wait_for_docker_ssh(port=port)

    @staticmethod
    def stop_docker_containers(configs, docker_client=None):
        client = docker_client if docker_client is not None else docker.from_env()

        for container_name in [configs.container_target_name, configs.container_remote_name]:
            # First see if the container is running
            container, is_running = IntegrationTestUtils.is_docker_container_running(
                container_name, client
            )
            if container and is_running:
                container.stop()

                # Wait for it to stop
                while True:
                    _, is_running = IntegrationTestUtils.is_docker_container_running(
                        container_name, client
                    )
                    if is_running:
                        WAIT_FOR_DOCKER_SSH_SLEEP_TIME
                        logger.info(
                            f"Test docker container is not yet stopped, "
                            f"sleeping for [{WAIT_FOR_DOCKER_SHUTDOWN_TIME}] seconds"
                        )
                        time.sleep(WAIT_FOR_DOCKER_SHUTDOWN_TIME)
                    else:
                        break

        if docker_client is None and client is not None:
            client.close()

    @staticmethod
    def wait_for_docker_ssh(port):
        while True:

            cmd = f"nc -v -w 1 localhost {port}"
            returncode, _, _ = Utils.run_bash_cmd(cmd=cmd, timeout_seconds=5)
            if returncode != 0:
                logger.info(
                    f"Test docker container is not yet listening for ssh connections on port={port}, "
                    f"sleeping for [{WAIT_FOR_DOCKER_SSH_SLEEP_TIME}] seconds"
                )
                time.sleep(WAIT_FOR_DOCKER_SSH_SLEEP_TIME)
            else:
                logger.info(
                    f"Test docker container is accepting connections ssh connections on port={port}"
                )
                break

    @staticmethod
    def write_app_configs(test_configs, app_configs: AppConfigs):
        c = app_configs.to_dict(skip_defaults=True)
        c = IntegrationTestUtils.remove_nulls_from_dict(c)
        output_path = os.path.join(test_configs.config_dir, test_configs.config_file)
        IntegrationTestUtils.write_yaml_file(output_path, c)

    @staticmethod
    def write_yaml_file(output_path, data):
        with open(output_path, "w") as fh:
            yaml.dump(data, fh)
