#!/usr/bin/env python
import pandas as pd

from config.constants import DATACENTER

# Use model for athena query cache, DB operations when available
from aws_athena_helper import DataCenter

import logging

logger = logging.getLogger()
logger.setLevel(logging.WARN)


class AWS:
    class Athena:
        dc = DataCenter(
            aws_profile_name=DATACENTER.AWS_PROFILE_NAME,
            athena_config=DATACENTER.CREDENTIALS.ATHENA,
        )

        @classmethod
        def query(cls, query, table):
            response = cls.dc.athena.run_query(query, table)
            result = response["df"]
            return result

    class S3:
        dc = DataCenter(
            aws_profile_name=DATACENTER.AWS_PROFILE_NAME,
            s3_config=DATACENTER.CREDENTIALS.S3,
        )

        @classmethod
        def upload(cls, local_path: str, remote_uri: str):
            return cls.dc.s3.upload(
                local_path, "{}/{}".format(DATACENTER.DW_ROOT_DIR, remote_uri)
            )
