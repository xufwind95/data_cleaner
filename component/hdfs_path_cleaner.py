#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:File Name: hdfs_path_cleaner
:Author: xufeng 
:Date: 2021-08-06 6:44 PM
:Version: v.1.0
:Description:
"""
import re
import traceback

from component.base_cleaner import BaseCleaner
from common.utils import ExpireTimeDesc, DateUtil, ShellUtil
from common.logger_adaptor import LogAdaptor


class HDFSPathCleaner(BaseCleaner):

    # default expire time is 4 month
    DEFAULT_EXPIRE_TIME = ExpireTimeDesc(0, 4, 0)

    def __init__(self,
                 logger: LogAdaptor,
                 hdfs_paths,
                 skip_trash=False,
                 ignore_update_time=False,
                 hdfs_day_time_pattern=r'\d{4}-\d{2}-\d{2}',
                 expire_time: ExpireTimeDesc = None):
        """
        delete hdfs path util
        :param logger:
        :param hdfs_paths: ['/user/proj/2021/input', 'user/proj/*/tmp' ...]
            if '*' in path, will first use `hadoop fs -ls ...` to list all paths
        :param skip_trash: skip trash flag to remove hadoop file
        :param ignore_update_time: if set true, will delete hadoop files with out check update time
        :param hdfs_day_time_pattern: the day time pattern in hdfs ls line
            ex: drwxr-xr-x   - proj hive          0 2021-07-21 18:29 /user/hive/warehouse/proj.db/tb1 => 2021-07-21
        :param expire_time: if ignore_update_time set true, this param will ignored, default 4 month
        """
        self._logger = logger
        self._hdfs_paths = hdfs_paths
        self._skip_trash = skip_trash
        self._ignore_update_time = ignore_update_time
        self._hdfs_day_time_pattern = hdfs_day_time_pattern
        self._expire_time = expire_time if expire_time else self.DEFAULT_EXPIRE_TIME

    def _check_and_update_param(self):
        if isinstance(self._hdfs_paths, str):
            self._hdfs_paths = [self._hdfs_paths]

        if not self._hdfs_paths:
            raise Exception(f"{self}: hdfs path:{self._hdfs_paths} is empty!")

    @property
    def description(self) -> str:
        return "hdfs path cleaner"

    def clean(self):
        self._check_and_update_param()

        for hdfs_path in self._hdfs_paths:

            if not hdfs_path:
                self._logger.error("find empty hdfs path, this script not support delete hole user directory!")
                continue

            if '*' in hdfs_path:
                self._handle_wildcard_character_path(hdfs_path)
                continue

            self._handle_common_path(hdfs_path)

    def _handle_common_path(self, hdfs_path):
        """
        check common path update time and execute delete
        :param hdfs_path:
        :return:
        """
        parent_path = self._get_parent_path(hdfs_path)

        hdfs_path_lines = self._ls_hdfs_path(parent_path)
        for path_line in hdfs_path_lines:
            time_str, tmp_path = self._get_ls_time_and_path(path_line)
            if tmp_path and (tmp_path in hdfs_path or hdfs_path in tmp_path):
                if self._is_expire(time_str):
                    self._exec_del(hdfs_path)
                return

        self._logger.warning(f"{self}: {hdfs_path} not found suitable path:{hdfs_path_lines}")

    def _handle_wildcard_character_path(self, hdfs_path):
        """
        if hdfs path has wildcard character, will find all real path first
        ex: /user/your_project/*/tmp/ will fist use
        `hdfs dfs -ls /user/your_project/*/tmp` to find real path
        :param hdfs_path:
        :return:
        """
        hdfs_path_lines = self._ls_hdfs_path(hdfs_path)

        if not hdfs_path_lines:
            self._logger.warning(f"hdfs path: {hdfs_path} get null on hdfs!")
            return

        for path_line in hdfs_path_lines:
            time_str, tmp_path = self._get_ls_time_and_path(path_line)
            if tmp_path and self._is_expire(time_str):
                    self._exec_del(tmp_path)

    def _get_parent_path(self, hdfs_path: str):
        """
        get current hdfs path's parent path
        ex:
            relative path like: project/tmp ==> project
            relative path like: project/tmp/ ==> project
            relative path like: project ==> ''
            absolute path like: /user/biz/project/tmp ==> /user/biz/project
            absolute path like: /user/biz/project/tmp/ ==> /user/biz/project
            hdfs path: hdfs://nameservice1/hive/warehouse/proj.db/tb1 ==> hdfs://nameservice1/hive/warehouse/proj.db
        :param hdfs_path:
        :return:
        """
        if "/" not in hdfs_path:
            return ''

        if hdfs_path[-1] == '/':
            hdfs_path = hdfs_path[:-1]

        is_absolute_path = True if hdfs_path[0] == '/' else False

        parent_path = '/'.join(hdfs_path.split('/')[:-1])

        if is_absolute_path and not parent_path.startswith('/'):
            parent_path = f'/{parent_path}'

        self._logger.info(f"hdfs path: {hdfs_path} parent path is:{parent_path}")
        return parent_path

    def _ls_hdfs_path(self, hdfs_path):
        shell_cmd = f"hadoop fs -ls {hdfs_path}"
        try:
            ret = ShellUtil.exec_shell_with_result(shell_cmd, self._logger)
            self._logger.debug(f"hdfs ls result is:")
            for line in ret:
                self._logger.debug(f"ls result: {line}")
            return ret
        except Exception:
            self._logger.error(f"{self} execute shell cmd error:{traceback.format_exc()}")
            return ['']

    def _get_ls_time_and_path(self, dfs_ls_line):
        """
        drwxr-xr-x   - proj hive          0 2021-07-21 18:29 /user/hive/warehouse/proj.db/tb1
        will return: 2021-07-21, /user/hive/warehouse/proj.db/tb1
        :param dfs_ls_line:
        :return:
        """
        time_pattern = re.search(self._hdfs_day_time_pattern, dfs_ls_line)
        time_str = '' if not time_pattern else time_pattern.group(0)
        hdfs_path = '' if not time_str else dfs_ls_line.strip().split(' ')[-1]

        self._logger.debug(f"ls line is: {dfs_ls_line}, ret is:{time_str}, {hdfs_path}")
        return time_str, hdfs_path

    def _is_expire(self, update_time_str):
        if self._ignore_update_time:
            return True

        if not self._expire_time:
            return True

        update_time = DateUtil.str_2_datetime(update_time_str)
        expire_time = self._get_expire_time()
        if DateUtil.compare_date(update_time, expire_time):
            return False
        return True

    def _get_expire_time(self):
        if not hasattr(self, 'expire_time'):
            expire_time = DateUtil.get_expire_time(self._expire_time)
            setattr(self, 'expire_time', expire_time)
        return getattr(self, 'expire_time')

    def _exec_del(self, hdfs_path):

        if self.test:
            return self._exec_del_test(hdfs_path)

        return self._real_exec_del(hdfs_path)

    def _exec_del_test(self, hdfs_path):
        trash_msg = 'with skip trash' if self._skip_trash else 'with trash'
        self._logger.info(f"{self.action_prefix}{self.description} delete hdfs path: {hdfs_path} {trash_msg}")

    def _real_exec_del(self, hdfs_path):
        skip_trash = '-skipTrash' if self._skip_trash else ''
        shell_cmd = f"hadoop fs -rm -r {skip_trash} {hdfs_path}"

        ret, msg = ShellUtil.exec_shell_with_status(shell_cmd, self._logger)
        if ret:
            self._logger.info(f"{self.action_prefix}{self} remove hdfs path:{hdfs_path} success")
            return
        self._logger.error(f"{self.action_prefix}{self} remove hdfs path:{hdfs_path} failed: {msg}")