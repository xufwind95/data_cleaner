#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: utils
:Author: xufeng@bbdservice.com 
:Date: 2021-08-06 6:32 PM
:Version: v.1.0
:Description:
"""
import subprocess
import tempfile
import traceback
from collections import namedtuple
from datetime import datetime
from dateutil.relativedelta import relativedelta

from common.logger_adaptor import LogAdaptor

ExpireTimeDesc = namedtuple("ExpireTimeDesc", ['year', 'month', 'day'])


class DateUtil:

    @staticmethod
    def timestamp_2_datetime(times):
        return datetime.fromtimestamp(times)

    @staticmethod
    def str_2_datetime(time_str, fmt='%Y-%m-%d'):
        return datetime.strptime(time_str, fmt)

    @staticmethod
    def get_expire_time(expire_time: ExpireTimeDesc):
        years = expire_time.year
        months = expire_time.month
        days = expire_time.day
        ret_time = datetime.now() + relativedelta(years=-years, months=-months, days=-days)
        return ret_time

    @staticmethod
    def compare_date(update_time: datetime, expire_time: datetime):
        return update_time.date() >= expire_time.date()


class ShellUtil:

    @staticmethod
    def exec_shell_with_result(shell_cmd: str, logger: LogAdaptor):
        """
        execute shell script and get the result
        error will raise if execute failed
        :param shell_cmd:
        :param logger:
        :return:
        """
        logger.info(f"shell command is: {shell_cmd}")
        out_temp = tempfile.SpooledTemporaryFile()
        file_no = out_temp.fileno()

        try:
            command = shell_cmd.strip()
            p = subprocess.Popen(command, stdout=file_no, stderr=file_no, shell=True)
            p.communicate()

            out_temp.seek(0)
            out_lines = [line.decode('unicode-escape').strip() for line in out_temp.readlines()]

            if p.returncode == 0:
                return out_lines

            error_msg = '\n'.join(out_lines)
            raise Exception(f"command: [{shell_cmd}], execute failed: {error_msg}")
        finally:
            if out_temp:
                out_temp.close()

    @staticmethod
    def exec_shell_with_status(shell_cmd: str, logger: LogAdaptor) -> (bool, str):
        """
        ignore subprocess result
        just return execute (success and '') or (failed and error msg)
        :param shell_cmd:
        :param logger:
        :return:
        """
        try:
            ShellUtil.exec_shell_with_result(shell_cmd, logger)
            return True, ''
        except Exception:
            err_msg = traceback.format_exc()
            return False, err_msg

    @staticmethod
    def exec_shell(shell_cmd: str, logger: LogAdaptor):
        """
        just execute shell script, ignore subprocess result and execute status
        :param shell_cmd:
        :param logger:
        :return:
        """
        ShellUtil.exec_shell_with_status(shell_cmd, logger)
