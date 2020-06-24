import logging
import requests
import socket
import subprocess
import time
from typing import Tuple


class Utils(object):

    @staticmethod
    def run_bash_cmd(cmd: str, timeout_seconds: int = None) -> Tuple[int, str, str]:
        result = subprocess.run(
            cmd, shell=True, timeout=timeout_seconds, capture_output=True, executable="/bin/bash"
        )

        stdout = result.stdout.decode("ascii").strip()
        stderr = result.stderr.decode("ascii").strip()
        return result.returncode, stdout, stderr

    @staticmethod
    def wait_for_http_service(
        logger: logging.Logger,
        host: str,
        port: int,
        path: str,
        wait_time: int,
        num_retries: int,
        timeout: int = 1,
    ) -> None:
        num_tries = 0
        while True:
            response = None
            try:
                if num_tries >= num_retries:
                    raise Exception(
                        f"exhausted number of retries waiting for http service to accept connections; host={host}, port={port}"
                    )
                num_tries += 1

                url = f"http://{host}:{port}{path}"
                response = requests.get(url)
                response.raise_for_status()
                if response.status_code == 200:
                    return
                else:
                    logger.info(
                        f"waiting to retry for tcp port to accept connections; wait_time={wait_time}, host={host}, port={port}: {e}"
                    )
                    time.sleep(wait_time)

            except Exception as e:
                logger.info(
                    f"waiting for http service to accept connections; host={host}, port={port}: {e}"
                )
            finally:
                if response is not None:
                    response.close()
