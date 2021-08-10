#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: util_test
:Author: xufeng@bbdservice.com 
:Date: 2021-08-06 6:53 PM
:Version: v.1.0
:Description:
"""
import os
from common.logger_adaptor import LogAdaptor
from common.utils import ShellUtil


def shell_util_test():
    logger = LogAdaptor()
    try:
        out = ShellUtil.exec_shell_with_result("cd 123", logger)
        print(out)
    except:
        import traceback
        print(traceback.format_exc())

    project_path = os.path.abspath('../')

    out = ShellUtil.exec_shell_with_result(f"ls '{project_path}'", logger)
    assert 'component' in out
    assert 'tests' in out


if __name__ == "__main__":
    shell_util_test()
