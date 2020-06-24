import logging
import sys
from dataclasses import dataclass, field
from rsyncdirector.lib.config import JobType
from invoke import run


class Rsync(object):
    def __init__(self, logger, type, sync, user=None, host=None, port=None, private_key_path=None):
        self.logger = logger
        self.type = type
        self.sync = sync
        self.user = user
        self.host = host
        self.port = port
        self.private_key_path = private_key_path

    def run(self):
        options = self.sync["opts"]
        remote_prefix = ""

        match self.type:
            case JobType.LOCAL:
                # Currently a noop as the command that we build is the proper one for local
                # synchronization.
                pass
            case JobType.REMOTE:
                remote_prefix = f"{self.user}@{self.host}:"

                ssh_opts = []
                if self.port:
                    ssh_opts.append(f"-p {self.port}")
                if self.private_key_path:
                    ssh_opts.append(f"-i {self.private_key_path}")
                if len(ssh_opts) > 0:
                    ssh_options = " ".join(ssh_opts)
                    options.extend(["-e", f"'ssh {ssh_options}'"])

            case _:
                raise Exception(f"invalid job type; self.type={self.type}")

        opts = " ".join(options)
        cmd = f"rsync {opts} {self.sync['source']} {remote_prefix}{self.sync['dest']}"
        self.logger.info(f"running rsync; cmd={cmd}")
        run(cmd)
