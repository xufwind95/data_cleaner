#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: logger_adaptor
:Author: xufeng@bbdservice.com 
:Date: 2021-08-06 6:35 PM
:Version: v.1.0
:Description:
"""
from logging import Logger


class LogAdaptor:

    def __init__(self, logger: Logger = None):
        self._logger = logger
        self.has_log = True if self._logger else False

    def debug(self, msg):
        self._echo_log(msg, 'debug')

    def info(self, msg):
        self._echo_log(msg, 'info')

    def warning(self, msg):
        self._echo_log(msg, 'warning')

    def error(self, msg):
        self._echo_log(msg, 'error')

    def _echo_log(self, msg, func_attr):
        if not self.has_log:
            print(msg)
            return

        func = getattr(self._logger, func_attr)
        func(msg)