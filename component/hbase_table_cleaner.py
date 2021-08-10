#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:File Name: hbase_table_cleaner
:Author: xufeng 
:Date: 2021-08-06 6:46 PM
:Version: v.1.0
:Description:
"""
import os
import re
import traceback

from component.base_cleaner import BaseCleaner
from common.utils import ExpireTimeDesc, DateUtil, ShellUtil
from common.logger_adaptor import LogAdaptor


class HbaseTableCleaner(BaseCleaner):

    DEFAULT_EXPIRE_TIME = ExpireTimeDesc(0, 4, 0)

    def __init__(self,
                 logger: LogAdaptor,
                 hbase_namespace,
                 hbase_data_dir='/hbase/data',
                 clear_tables=None,
                 drop_table: bool = False,
                 ignore_update_time: bool = False,
                 hdfs_day_time_pattern=r'\d{4}-\d{2}-\d{2}',
                 expire_time: ExpireTimeDesc = None):
        """
        hbase table cleaner
        :param logger:
        :param hbase_namespace:
        :param hbase_data_dir: hbase table data dir on hdfs:
            ex: /hbase/data/proj1_namespace/tb1_20210801
            if expire time set, will use hdfs update time to check table expire time
        :param clear_tables: tables need cleaner, support three type:
            '*' or ['*'] will delete all table
            ['tb1', 'tb2'] or 'tb1' will delete the specified
            ['tb*'] or 'tb*' will delete the table begin with tb
        :param drop_table: if set true, will execute `drop namespace:table_name`
        :param ignore_update_time: if set true, clear will not check update time
        :param hdfs_day_time_pattern:
        :param expire_time: default four month
        """
        self._logger = logger
        self._hbase_namespace = hbase_namespace
        self._hbase_data_dir = hbase_data_dir
        self._clear_tables = clear_tables
        self._drop_table = drop_table
        self._ignore_update_time = ignore_update_time
        self._hdfs_day_time_pattern = hdfs_day_time_pattern
        self._expire_time = expire_time

        self._namespace_tables = None
        self._table_hdfs_update_time = None

    def _check_and_update_param(self):

        if not self._hbase_namespace:
            raise Exception(f"{self} no hbase namespace specified")

        if not self._clear_tables:
            raise Exception(f"{self} no clear table specified!")

        if isinstance(self._clear_tables, str):
            self._clear_tables = [self._clear_tables]

        if not self._expire_time:
            self._expire_time = self.DEFAULT_EXPIRE_TIME

    @property
    def description(self) -> str:
        return "hbase table cleaner"

    def clean(self):
        self._check_and_update_param()
        self._get_all_table_in_namespace()
        self._get_table_update_time_on_hdfs()

        if '*' in self._clear_tables:
            self._logger.info(f"{self} handle all table in namespace: {self._hbase_namespace}")
            self._handle_all_table()
            return

        for table_name in self._clear_tables:
            if '*' in table_name:
                self._handle_wildcard_table(table_name)
                continue

            if table_name not in self._namespace_tables:
                self._logger.warning(f"{self} found table {table_name} not in namespace!")
                continue

            self._handle_single_table(table_name)

    def _get_all_table_in_namespace(self):
        lines = self._exec_hbase_shell('list')
        self._logger.info(f"{self} all table in hbase:{lines}")
        self._namespace_tables = []

        name_space_prfix = f'{self._hbase_namespace}:'
        for line in lines:
            line_ = line.strip()
            if line_.startswith(name_space_prfix):
                table_name = line_.split(name_space_prfix)[-1]
                if table_name:
                    self._namespace_tables.append(table_name)
        self._logger.info(f"{self} hbase namespace:{self._hbase_namespace} tables:{self._namespace_tables}")

    def _get_table_update_time_on_hdfs(self):
        if self._ignore_update_time:
            return

        namespace_dir = os.path.join(self._hbase_data_dir, self._hbase_namespace)
        if namespace_dir[-1] != '/':
            namespace_dir = f'{namespace_dir}/'
        shell_cmd = f"hadoop fs -ls {namespace_dir}"
        try:
            ls_lines = ShellUtil.exec_shell_with_result(shell_cmd, self._logger)
            info = {}
            for line in ls_lines:
                if namespace_dir in line:
                    update_date_time, _ = self._get_ls_time_and_path(line)
                    table_name = line.strip().split(namespace_dir)[-1]
                    info[table_name] = update_date_time
            self._table_hdfs_update_time = info
            self._logger.info(
                f"{self} namespace: {self._hbase_namespace}, table update time is:{self._table_hdfs_update_time}")
        except Exception:
            self._logger.error(f"{self} ls warehouse dir error:{traceback.format_exc()}")
            raise

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

    def _handle_all_table(self):
        for table in self._namespace_tables:
            self._handle_single_table(table)

    def _handle_wildcard_table(self, wildcard_table_name):
        reg = wildcard_table_name.replace('*', r'[\w]*?')
        self._logger.info(f"{self}: {wildcard_table_name} reg is:{reg}")

        tables = [it for it in self._namespace_tables if re.search(reg, it)]

        if not tables:
            self._logger.warning(f"{self}: {wildcard_table_name} no match table found!")
            return

        self._logger.info(f"{self}: {wildcard_table_name} found table:{tables}")

        for table_name in tables:
            self._handle_single_table(table_name)

    def _handle_single_table(self, table_name):
        if not self._is_expire(table_name):
            self._logger.info(f"{self} table: {table_name} not expire, do nothing")
            return

        self._exec_del(table_name)

    def _is_expire(self, table_name):
        if self._ignore_update_time:
            return True

        if not self._expire_time:
            return True

        update_time_str = self._table_hdfs_update_time.get(table_name, None)
        if not update_time_str:
            self._logger.warning(f"{self} hbase table:{table_name}, not found update time")
            return False

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

    def _exec_hbase_shell(self, shell_commands):
        """
        execute hbase shell command, if error happened, exception will raise directly
        :param shell_commands: each item in shell_commands must a whole command
        :return:
        """
        if isinstance(shell_commands, str):
            shell_commands = [shell_commands]

        if 'exit' not in shell_commands:
            shell_commands.append('exit')

        command_text = '\n'.join(shell_commands)
        self._logger.info(f"{self} hbase shell command is:\n{command_text}")
        tmp_file_name = 'tmp_cmd.shell'

        with open(tmp_file_name, 'w') as f:
            for command in shell_commands:
                f.write(command)
                f.write('\n')

        tmp_file_path = os.path.abspath(tmp_file_name)
        exec_shell_command = f"hbase shell {tmp_file_path}"
        try:
            ret = ShellUtil.exec_shell_with_result(exec_shell_command, self._logger)
            return ret
        finally:
            if os.path.exists(tmp_file_name):
                os.remove(tmp_file_path)

    def _exec_del(self, table_name):

        if self.test:
            return self._exec_del_test(table_name)

        return self._real_exec_del(table_name)

    def _exec_del_test(self, table_name):
        self._logger.info(f"{self.action_prefix}{self} test clear table: {table_name}, drop table:{self._drop_table}")

    def _real_exec_del(self, table_name):
        name_space_table = f'{self._hbase_namespace}:{table_name}'
        commands = [f"disable '{name_space_table}'"]
        if self._drop_table:
            commands.append(f"drop '{name_space_table}'")

        try:
            self._exec_hbase_shell(commands)
            self._logger.info(f"{self.action_prefix}{self} clear table:{name_space_table} success")
        except Exception:
            self._logger.error(f"{self.action_prefix}{self} clear "
                               f"table:{name_space_table} failed:{traceback.format_exc()}")

