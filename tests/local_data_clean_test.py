#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:File Name: local_data_clean_test
:Author: xufeng
:Date: 2021-08-10 5:40 PM
:Version: v.1.0
:Description:
"""
import os

from cleaner import ProjectCleanerBuilder
from common.utils import ShellUtil, LogAdaptor


def test_delete_local_path():

    current_path = os.path.abspath('.')
    test_dir = os.path.join(current_path, 'test_dir')
    log_file_path = os.path.join(test_dir, 't1.log')
    out_file_path = os.path.join(test_dir, 't2.out')
    text_file_path = os.path.join(test_dir, 't3.text')
    os.makedirs(test_dir, exist_ok=True)

    create_dir_file_cmd = [
        f"echo 'test1' > {log_file_path}",
        f"echo 'test2' > {out_file_path}",
        f"echo 'hello world' > {text_file_path}"
    ]

    cmd = '\n'.join(create_dir_file_cmd)
    log = LogAdaptor()

    ret, msg = ShellUtil.exec_shell_with_status(cmd, log)

    assert ret

    pc = ProjectCleanerBuilder() \
        .with_local_paths(local_paths=test_dir,
                          delete_all_file=False,
                          default_suffix=['.log', '.out'],
                          ignore_update_time=True
                          ) \
        .build()
    pc.clean()

    assert os.path.exists(text_file_path)
    assert not os.path.exists(log_file_path)
    assert not os.path.exists(out_file_path)

    os.remove(text_file_path)
    os.rmdir(test_dir)


if __name__ == "__main__":
    test_delete_local_path()
