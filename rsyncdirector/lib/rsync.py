# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

import multiprocessing
from rsyncdirector.lib.logging import Logger, LogStreamer
from rsyncdirector.lib.config import JobType
from rsyncdirector.lib.enums import RunResult
from invoke import run
from typing import Dict


class Rsync(object):
    def __init__(
        self,
        logger: Logger,
        result_queue: multiprocessing.Queue,
        type: JobType,
        sync: Dict,
        user: str | None = None,
        host: str | None = None,
        port: str | None = None,
        private_key_path: str | None = None,
    ):
        self.logger = logger
        self.result_queue = result_queue
        self.type = type
        self.sync = sync
        self.user = user
        self.host = host
        self.port = port
        self.private_key_path = private_key_path
        self.log_streamer = LogStreamer(self.logger, "rsync_out")

    def run(self) -> None:
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
        self.logger.info("running sync", cmd=cmd)
        # If we find that the output is lagging or coming in huge, delayed chunks, we can try adding
        # pty=True to the ctx.run() call. This fools rsync into thinking it is talking to a real
        # terminal, which often forces it to flush its output buffer more frequently.
        result = run(cmd, warn=True, out_stream=self.log_streamer, err_stream=self.log_streamer)
        run_result = RunResult.FAIL
        if result and result.ok:
            run_result = RunResult.SUCCESS
        self.result_queue.put((run_result, result))
