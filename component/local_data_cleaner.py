#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:File Name: local_data_cleaner
:Author: xufeng 
:Date: 2021-08-06 6:40 PM
:Version: v.1.0
:Description:
"""
import os
import traceback

from component.base_cleaner import BaseCleaner
from common.utils import ExpireTimeDesc, DateUtil
from common.logger_adaptor import LogAdaptor


class LocalDataCleaner(BaseCleaner):

    # default expire time is 15 day
    DEFAULT_EXPIRE_TIME = ExpireTimeDesc(0, 0, 15)
    DEFAULT_FILE_SUFFIX = ['.log', '.out']
    DEL_TYPE_FILE = 1
    DEL_TYPE_DIR = 2

    def __init__(self,
                 logger: LogAdaptor,
                 local_paths,
                 delete_all_file=False,
                 ignore_delete_type=False,
                 default_suffix=None,
                 ignore_update_time=False,
                 expire_time: ExpireTimeDesc = None):
        """
        delete local file util
        ex: delete expire log, tmp file etc.
        :param logger:
        :param local_paths: ['/opt/app/logs', 'opt/app/tmp/'] or '/opt/app/xxx.log'
        :param delete_all_file: if delete all file is true,
            file will delete with out check file type and file update time
        :param ignore_delete_type: if set true, file suffix will not check
        :param default_suffix: if ignore_delete_type set true, this param will ignored
            default file suffix
            ex: .log, .out , if set .log, xxx.log.202106 also can be delete
        :param ignore_update_time: if set true, file update time will not check
        :param expire_time: if ignore_update_time set true, this param will ignored, default 15 day
        """
        self._logger = logger
        self._clear_local_paths = local_paths
        self._delete_all = delete_all_file
        self._ignore_delete_type = ignore_delete_type
        self._default_file_suffix = default_suffix if default_suffix else self.DEFAULT_FILE_SUFFIX
        self._ignore_update_time = ignore_update_time
        self._expire_time = expire_time if expire_time else self.DEFAULT_EXPIRE_TIME

    def _check_and_update_param(self):
        if not self._clear_local_paths:
            raise Exception(f"{self}: local path:{self._clear_local_paths} is empty!")

        if isinstance(self._clear_local_paths, str):
            self._clear_local_paths = [self._clear_local_paths]

        if isinstance(self._default_file_suffix, str):
            self._default_file_suffix = [self._default_file_suffix]

    @property
    def description(self) -> str:
        return "local file cleaner"

    def clean(self):
        self._check_and_update_param()

        for path in self._clear_local_paths:
            if os.path.isfile(path):
                self._handle_file(path)
                continue

            if os.path.isdir(path):
                self._handle_dir(path)
                continue

            self._logger.warning(f"path {path} not found!")

    def _handle_file(self, file_path):
        if self._can_delete(file_path):
            self._exec_del(file_path, self.DEL_TYPE_FILE)

    def _can_delete(self, file_path):

        # if delete all is set, file will delete with out other check
        if self._is_del_all_set():
            return True

        # check file suffix and update time
        if self._is_del_type(file_path) and self._is_expire(file_path):
            return True

        return False

    def _handle_dir(self, dir_path):

        for root, dirs, files in os.walk(dir_path):

            for f in files:
                self._handle_file(os.path.join(root, f))

            for d in dirs:
                self._handle_dir(os.path.join(root, d))

        # if dir is empty, delete it
        if not os.listdir(dir_path):
            self._exec_del(dir_path, self.DEL_TYPE_DIR)
            return

    def _is_del_all_set(self):
        return self._delete_all

    def _is_del_type(self, file_path) -> bool:
        """
        if ignore file suffix is set true, file suffix will not check
        check file type in specified delete type
        ex:　if .log　or　nohup.out is specified, only delete this two type
        :param file_path:
        :return:
        """
        if self._ignore_delete_type:
            return True

        if not self._default_file_suffix:
            return True

        file_name = os.path.basename(file_path)
        for suffix in self._default_file_suffix:
            if suffix in file_name:
                return True

        return False

    def _is_expire(self, file_path):
        """
        check file update time
        if early than specified expire time return true
        else return false
        :param file_path:
        :return:
        """
        if self._ignore_update_time:
            return True

        if not self._expire_time:
            return True

        mtime = os.path.getmtime(file_path)

        file_update_time = DateUtil.timestamp_2_datetime(mtime)
        expire_time = self._get_expire_time()

        if DateUtil.compare_date(file_update_time, expire_time):
            return False

        return True

    def _get_expire_time(self):
        if not hasattr(self, 'expire_time'):
            expire_time = DateUtil.get_expire_time(self._expire_time)
            setattr(self, 'expire_time', expire_time)
        return getattr(self, 'expire_time')

    def _exec_del(self, file_path_or_dir, del_type):
        """
        delete directory or file
        :param file_path_or_dir:
        :param del_type:
        :return:
        """
        if self.test:
            return self._exec_del_test(file_path_or_dir, del_type)

        return self._real_exec_del(file_path_or_dir, del_type)

    def _exec_del_test(self, file_path_or_dir, del_type):
        del_type = "file" if del_type == self.DEL_TYPE_FILE else "dir"
        self._logger.info(f"{self.action_prefix}{self.description} delete {del_type}: {file_path_or_dir}")

    def _real_exec_del(self, file_path_or_dir, del_type):
        """
        us python os api remove file or directory
        :param file_path_or_dir:
        :param del_type:
        :return:
        """
        assert del_type in (self.DEL_TYPE_FILE, self.DEL_TYPE_DIR), f'delete type error:{del_type}'

        try:
            if del_type == self.DEL_TYPE_FILE:
                file_type = "file"
                os.remove(file_path_or_dir)
            else:
                file_type = 'dir'
                os.rmdir(file_path_or_dir)

            self._logger.info(f"{self.action_prefix}{self.description} remove {file_type}: {file_path_or_dir} success")
        except Exception:
            self._logger.error(
                f'{self.action_prefix}{self.description} remove file or dir'
                f': {file_path_or_dir} failed: {traceback.format_exc()}')
