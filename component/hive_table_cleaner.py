#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: hive_table_cleaner
:Author: xufeng@bbdservice.com 
:Date: 2021-08-06 6:45 PM
:Version: v.1.0
:Description:
"""
import os
import re
import traceback

from component.base_cleaner import BaseCleaner
from common.utils import ExpireTimeDesc, DateUtil, ShellUtil
from common.logger_adaptor import LogAdaptor


class HiveTableCleaner(BaseCleaner):

    DEFAULT_EXPIRE_TIME = ExpireTimeDesc(0, 4, 0)
    CHECK_TIME_PARTITION_FIELD = 'partition_field'
    CHECK_TIME_HDFS_UPDATE_TIME = 'hdfs_update_time'

    DELETE_TYPE_TABLE = 1
    DELETE_TYPE_PARTITION = 2

    def __init__(self,
                 logger: LogAdaptor,
                 spark,
                 hive_db_name,
                 hive_db_warehouse_path,
                 hive_cmd="hive -e",
                 is_inner_table: bool = True,
                 skip_trash=False,
                 clear_tables=None,
                 ignore_update_time=False,
                 check_time_type=None,
                 partition_field_format='%Y%m%d',
                 hdfs_day_time_pattern=r'\d{4}-\d{2}-\d{2}',
                 expire_time: ExpireTimeDesc = None):
        """
        clear hive data util
        if drop partition, support signal partition
        :param logger:
        :param spark: active SparkSession
        :param hive_db_name:
        :param hive_db_warehouse_path: hive db dir on hdfs
        :param hive_cmd: execute hive command in shell
        :param is_inner_table: specify hive table is inner table or outer table, default inner table
        :param skip_trash: default false, if set true, will remove table or partition with skip trash
        :param clear_tables: table need clear data
        :param ignore_update_time: default false, all table deleted if set true
        :param check_time_type: check expire time type
            include `partition_field` and `hdfs_update_time` default `partition_field`
            check time by partition for time sorted partition if partition_field set
            check time by partition updatetime on hdfs if hdfs_update_time set
        :param partition_field_format: datetime format for time sorted partition field
        :param hdfs_day_time_pattern: the time regular expressions pattern for hdfs ls return
        :param expire_time:
        """
        self._logger = logger
        self._spark = spark
        self._hive_db_name = hive_db_name
        self._hive_db_warehouse_path = hive_db_warehouse_path
        self._hive_cmd = hive_cmd
        self._is_inner_table = is_inner_table
        self._skip_trash = skip_trash
        self._clear_tables = clear_tables
        self._ignore_update_time = ignore_update_time
        self._check_time_type = check_time_type
        self._partition_field_format = partition_field_format
        self._hdfs_day_time_pattern = hdfs_day_time_pattern
        self._expire_time = expire_time if expire_time else self.DEFAULT_EXPIRE_TIME

        self._db_tables = None
        self._table_hdfs_update_time = None

    def _check_and_update_param(self):
        if not self._spark:
            raise Exception(f"{self} spark not specified!")

        if not self._hive_db_name:
            raise Exception(f"{self} hive_db_name is not specified!")

        if not self._clear_tables:
            raise Exception(f"{self} no hive table specified")

        if isinstance(self._clear_tables, str):
            self._clear_tables = [self._clear_tables]

        if self._hive_db_warehouse_path and self._hive_db_warehouse_path[-1] != '/':
            self._hive_db_warehouse_path = f"{self._hive_db_warehouse_path}/"

        if not self._check_time_type:
            self._check_time_type = self.CHECK_TIME_PARTITION_FIELD

        if self._check_time_type not in(self.CHECK_TIME_PARTITION_FIELD, self.CHECK_TIME_HDFS_UPDATE_TIME):
            raise Exception(f"{self} check time type is invalid:{self._check_time_type}")

        if self._check_time_type == self.CHECK_TIME_PARTITION_FIELD:
            if not self._partition_field_format:
                raise Exception(f"{self} partition field format is not specified")
        else:
            if not self._hdfs_day_time_pattern:
                raise Exception(f"{self} hdfs day time pattern is not specified")

        self._expire_time = self.DEFAULT_EXPIRE_TIME if not self._expire_time else self._expire_time

    @property
    def description(self) -> str:
        return "hive table cleaner"

    def clean(self):
        self._check_and_update_param()

        self._get_all_table_name_in_db()

        if '*' in self._clear_tables:
            self._logger.info(f"{self}, '*' in clear tables, all table will execute clean")
            self._handle_clear_all_table()
            return

        for table_name in self._clear_tables:
            # check table
            if not table_name:
                self._logger.warning(f"{self} no table found!")
                continue

            if '*' in table_name:
                self._handle_wildcard_character_table(table_name)
                continue

            self._handle_single_table(table_name)

    def _get_all_table_name_in_db(self):
        """
        get all hive table in specified hive database,
        if no database or database have no table will raise exception
        :return:
        """
        self._db_tables = self._spark.sql(f"show tables in {self._hive_db_name}")\
            .rdd.map(lambda x: x.tableName).collect()

    def _get_table_update_time_on_hdfs(self):
        shell_cmd = f"hadoop fs -ls {self._hive_db_warehouse_path}"
        try:
            ls_lines = ShellUtil.exec_shell_with_result(shell_cmd, self._logger)
            info = {}
            for line in ls_lines:
                if self._hive_db_warehouse_path in line:
                    update_date_time, _ = self._get_ls_time_and_path(line)
                    table_name = line.strip().split(self._hive_db_warehouse_path)[-1]
                    info[table_name] = update_date_time
            self._table_hdfs_update_time = info
        except Exception:
            self._logger.error(f"{self} ls warehouse dir error:{traceback.format_exc()}")
            raise

    def _handle_clear_all_table(self):
        for table_name in self._db_tables:
            self._handle_single_table(table_name)

    def _handle_wildcard_character_table(self, wildcard_table_name):
        reg = wildcard_table_name.replace('*', r'[\w]*?')
        self._logger.info(f"{self}: {wildcard_table_name} reg is:{reg}")

        tables = [it for it in self._db_tables if re.search(reg, it)]

        if not tables:
            self._logger.warning(f"{self}: {wildcard_table_name} no match table found!")
            return

        self._logger.info(f"{self}: {wildcard_table_name} found table:{tables}")

        for table_name in tables:
            self._handle_single_table(table_name)

    def _handle_single_table(self, table_name):
        """
        TODO: add handler to chain of responsibility
        :param table_name:
        :return:
        """
        if table_name not in self._db_tables:
            self._logger.error(f"{self}, table: {table_name} not in db:{self._hive_db_name}")
            return

        if self._ignore_update_time:
            self._drop_table(table_name)
            return

        has_partition, partitions = self._check_and_get_table_partition(table_name)
        if not has_partition:
            self._handle_no_partition_table(table_name)
            return

        if not partitions:
            return

        if self._check_time_type == self.CHECK_TIME_PARTITION_FIELD and self._is_day_time_partition(partitions):
            self._handle_for_partition_field(table_name, partitions)
            return

        if self._check_time_type == self.CHECK_TIME_HDFS_UPDATE_TIME:
            self._handle_for_hdfs_update_time(table_name, partitions)
            return

    def _check_and_get_table_partition(self, table_name):
        try:
            partitions = self._spark.sql(f"show partitions {self._hive_db_name}.{table_name}")\
                .rdd\
                .map(lambda r: r[0])\
                .collect()
            return True, partitions
        except Exception:
            return False, None

    def _is_day_time_partition(self, partitions) -> bool:
        part0 = partitions[0].split('=')[-1]
        try:
            DateUtil.str_2_datetime(part0, self._partition_field_format)
            return True
        except Exception:
            return False

    def _handle_no_partition_table(self, table_name):
        if not self._table_hdfs_update_time:
            self._get_table_update_time_on_hdfs()

        update_time_str = self._table_hdfs_update_time.get(table_name, None)
        if not update_time_str:
            self._logger.warning(f"{self} not found {table_name} on warehouse path:{self._hive_db_warehouse_path}")
            return

        if self._is_expire(update_time_str):
            self._drop_table(table_name)

    def _drop_table(self, table_name):
        return self._exec_del(table_name, self.DELETE_TYPE_TABLE)

    def _handle_for_partition_field(self, table_name, partitions):
        """
        :param table_name:
        :param partitions: 'dt=20210721'
        :return:
        """
        expire_partitions = []
        for partition_str in partitions:
            partition = partition_str.split('=')[-1]
            if self._is_expire(partition, self._partition_field_format):
                expire_partitions.append(partition_str)
        self._logger.info(f"{self} table: {self._hive_db_name}.{table_name}, expire partitions: {expire_partitions}")

        if not expire_partitions:
            self._logger.info(f"{self} table:{self._hive_db_name}.{table_name} no expire partition found")
            return

        expire_dirs = [] if not self._skip_trash else [
            os.path.join(self._hive_db_warehouse_path, table_name, partition) for partition in expire_partitions]

        self._exec_del(table_name,
                       self.DELETE_TYPE_PARTITION,
                       max_delete_partition=max(expire_partitions),
                       delete_hdfs_dirs=expire_dirs)

    def _handle_for_hdfs_update_time(self, table_name, partitions):
        table_hdfs_dir = os.path.join(self._hive_db_warehouse_path, table_name)
        shell_cmd = f"hadoop fs -ls {table_hdfs_dir}"
        try:
            lines = ShellUtil.exec_shell_with_result(shell_cmd, self._logger)

            expire_partition_dirs = []
            for line in lines:
                if table_hdfs_dir in line:
                    update_time_str, partition_dir = self._get_ls_time_and_path(line)
                    if self._is_expire(update_time_str):
                        expire_partition_dirs.append(partition_dir)
            if not expire_partition_dirs:
                self._logger.info(f"{self} table: {table_name} no partition expire")
                return

            expire_partitions = []
            for partition_dir in expire_partition_dirs:
                for partition in partitions:
                    if partition_dir.endswith(partition):
                        expire_partitions.append(partition)

            self._exec_del(
                table_name,
                self.DELETE_TYPE_PARTITION,
                is_time_sorted_partition=False,
                delete_partitions=expire_partitions,
                delete_hdfs_dirs=expire_partition_dirs
            )
        except Exception:
            self._logger.error(f"{self}, ls table hdfs dir failed:{traceback.format_exc()}")

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

    def _is_expire(self, update_time_str, fmt='%Y-%m-%d'):
        if self._ignore_update_time:
            return True

        if not self._expire_time:
            return True

        update_time = DateUtil.str_2_datetime(update_time_str, fmt)
        expire_time = self._get_expire_time()
        if DateUtil.compare_date(update_time, expire_time):
            return False
        return True

    def _get_expire_time(self):
        if not hasattr(self, 'expire_time'):
            expire_time = DateUtil.get_expire_time(self._expire_time)
            setattr(self, 'expire_time', expire_time)
        return getattr(self, 'expire_time')

    def _exec_del(self,
                  table_name,
                  del_type,
                  is_time_sorted_partition=True,
                  max_delete_partition=None,
                  delete_partitions=None,
                  delete_hdfs_dirs=None):
        """
        drop table or drop table partition, del type in (self.DELETE_TYPE_TABLE, self.DELETE_TYPE_PARTITION)
        drop table command:
            drop table if exists table_name [purge]
        drop time sorted partitions command:
            alter table {table_name} drop partition (dt<{min_remain_partition}) [purge]
        drop specified partitions command:
            alter table {table_name} drop partition (dt = {delete_partitions[0]}) [purge]
            alter table {table_name} drop partition (dt = {delete_partitions[1]}) [purge]
        :param table_name:
        :param del_type: in (self.DELETE_TYPE_TABLE, self.DELETE_TYPE_PARTITION)
        :param is_time_sorted_partition: partition is time sorted
        :param max_delete_partition: specified when partition is time sorted, format: dt=20210721
        :param delete_partitions: partitions need to delete, specified when partition is not time sorted
            format: dt=20210721
        :param delete_hdfs_dirs: partition dir on hdfs, dir will remove if skip trash set
        :return:
        """
        if self.test:
            return self._exec_del_test(
                table_name,
                del_type,
                is_time_sorted_partition,
                max_delete_partition,
                delete_partitions,
                delete_hdfs_dirs)

        return self._real_exec_del(
            table_name,
            del_type,
            is_time_sorted_partition=is_time_sorted_partition,
            max_delete_partition=max_delete_partition,
            delete_partitions=delete_partitions,
            delete_hdfs_dirs=delete_hdfs_dirs)

    def _exec_del_test(self,
                       table_name,
                       del_type,
                       is_time_sorted_partition,
                       max_delete_partition,
                       delete_partitions,
                       delete_hdfs_dirs):

        if not self._is_inner_table:
            delete_hdfs_dirs = os.path.join(self._hive_db_warehouse_path, table_name) if \
                del_type == self.DELETE_TYPE_TABLE else delete_hdfs_dirs
            trash_msg = f'with skip trash,dir is {delete_hdfs_dirs}' if self._skip_trash else 'with trash'
        else:
            trash_msg = f'with purge data' if self._skip_trash else 'with not purge data'
        if del_type == self.DELETE_TYPE_TABLE:
            msg = f"{self.action_prefix}{self} drop table {self._hive_db_name}.{table_name} {trash_msg}"
        elif is_time_sorted_partition:
            msg = f"{self.action_prefix}{self} drop table {self._hive_db_name}.{table_name} " \
                f"which partition <= {max_delete_partition} {trash_msg}"
        else:
            msg = f"{self.action_prefix}{self} drop table {self._hive_db_name}.{table_name} " \
                f"with partitions:{delete_partitions} {trash_msg}"

        self._logger.info(msg)

    def _real_exec_del(self,
                       table_name,
                       del_type,
                       is_time_sorted_partition=True,
                       max_delete_partition=None,
                       delete_partitions=None,
                       delete_hdfs_dirs=None):

        if del_type == self.DELETE_TYPE_TABLE:
            return self._exec_del_table(table_name)

        if is_time_sorted_partition:
            return self._exec_del_sorted_partition(table_name, max_delete_partition, delete_hdfs_dirs)

        return self._exec_del_partitions(table_name, delete_partitions, delete_hdfs_dirs)

    def _exec_del_table(self, table_name):
        if self._is_inner_table:
            return self._real_exec_del_inner_table(table_name)
        return self._real_exec_del_outer_table(table_name)

    def _real_exec_del_inner_table(self, table_name):

        drop_cmd = f"drop table if exists {self._hive_db_name}.{table_name}"
        drop_cmd = f'{drop_cmd} purge' if self._skip_trash else drop_cmd
        try:
            self._build_and_exec_hive_command(drop_cmd)
            self._spark.sql(drop_cmd)
            self._logger.warning(
                f"{self.action_prefix}{self} drop inner table {self._hive_db_name}.{table_name} success")
        except Exception:
            self._logger.error(
                f"{self.action_prefix}{self} drop inner "
                f"table {self._hive_db_name}.{table_name} failed:{traceback.format_exc()}")

    def _real_exec_del_outer_table(self, table_name):

        if self._skip_trash:
            hdfs_table_path = os.path.join(self._hive_db_warehouse_path, table_name)
            self._logger.info(
                f"{self.action_prefix}{self} remove "
                f"table:{self._hive_db_name}.{table_name} from hdfs path:{hdfs_table_path}")
            self._delete_with_skip_trash(hdfs_table_path)

        try:
            drop_cmd = f"drop table if exists {self._hive_db_name}.{table_name}"
            self._build_and_exec_hive_command(drop_cmd)
            self._logger.warning(f"{self.action_prefix}{self} drop outer "
                                 f"table {self._hive_db_name}.{table_name} success")
        except Exception:
            self._logger.error(
                f"{self.action_prefix}{self} drop outer table "
                f"{self._hive_db_name}.{table_name} failed:{traceback.format_exc()}")

    def _exec_del_sorted_partition(self, table_name, max_delete_partition, delete_hdfs_dirs):
        if self._is_inner_table:
            return self._real_exec_del_sorted_partition_inner(table_name, max_delete_partition)
        return self._real_exec_del_sorted_partition_outer(table_name, max_delete_partition, delete_hdfs_dirs)

    def _real_exec_del_sorted_partition_inner(self, table_name, max_delete_partition):

        try:
            items = max_delete_partition.split('=')
            partition_field, partition = items[0], items[-1]
            drop_cmd = [
                'alter table',
                f'{self._hive_db_name}.{table_name}',
                'drop partition',
                f"""({partition_field} <= '{partition}')""",
                'purge' if self._skip_trash else ''
            ]
            self._build_and_exec_hive_command(drop_cmd)
            self._logger.info(f"{self.action_prefix}{self} drop inner table {table_name} partition success!")
        except Exception:
            self._logger.error(f"{self.action_prefix}{self} drop inner "
                               f"table {table_name} partition failed:{traceback.format_exc()}")

    def _real_exec_del_sorted_partition_outer(self, table_name, max_delete_partition, delete_hdfs_dirs):

        if self._skip_trash and delete_hdfs_dirs:
            for hdfs_dir in delete_hdfs_dirs:
                self._delete_with_skip_trash(hdfs_dir)

        try:
            items = max_delete_partition.split('=')
            partition_field, partition = items[0], items[-1]
            drop_cmd = [
                'alter table',
                f'{self._hive_db_name}.{table_name}',
                'drop partition',
                f"""({partition_field} <= '{partition}')"""
            ]
            self._build_and_exec_hive_command(drop_cmd)
            self._logger.info(f"{self.action_prefix}{self} drop outer table {table_name} partition success!")
        except Exception:
            self._logger.error(f"{self.action_prefix}{self} drop outer "
                               f"table {table_name} partition failed:{traceback.format_exc()}")

    def _exec_del_partitions(self, table_name, delete_partitions, delete_hdfs_dirs):
        if self._is_inner_table:
            return self._real_exec_del_partitions_inner(table_name, delete_partitions)
        return self._real_exec_del_partitions_outer(table_name, delete_partitions, delete_hdfs_dirs)

    def _real_exec_del_partitions_inner(self, table_name, delete_partitions):
        try:
            for partition in delete_partitions:
                items = partition.split('=')
                partition_field, partition = items[0], items[-1]
                drop_cmd = [
                    'alter table',
                    f'{self._hive_db_name}.{table_name}',
                    'drop partition',
                    f"""({partition_field}='{partition}')"""
                    'purge' if self._skip_trash else ''
                ]
                self._build_and_exec_hive_command(drop_cmd)
            self._logger.info(f"{self.action_prefix}{self} drop inner table {table_name} partition success!")
        except Exception:
            self._logger.error(f"{self.action_prefix}{self} drop inner "
                               f"table {table_name} partition failed:{traceback.format_exc()}")

    def _real_exec_del_partitions_outer(self, table_name, delete_partitions, delete_hdfs_dirs):
        if self._skip_trash and delete_hdfs_dirs:
            for hdfs_dir in delete_hdfs_dirs:
                self._delete_with_skip_trash(hdfs_dir)

        try:
            for partition in delete_partitions:
                items = partition.split('=')
                partition_field, partition = items[0], items[-1]
                drop_cmd = [
                    'alter table',
                    f'{self._hive_db_name}.{table_name}',
                    'drop partition',
                    f"""({partition_field}='{partition}')"""
                ]
                self._build_and_exec_hive_command(drop_cmd)
            self._logger.info(f"{self.action_prefix}{self} drop outer table {table_name} partition success!")
        except Exception:
            self._logger.error(f"{self.action_prefix}{self} drop outter "
                               f"table {table_name} partition failed:{traceback.format_exc()}")

    def _build_and_exec_hive_command(self, command_items):
        if isinstance(command_items, str):
            command_items = [command_items]
        exec_cmd = ' '.join(command_items)
        if self._hive_cmd not in exec_cmd:
            exec_cmd = f'{self._hive_cmd} "{exec_cmd}"'
        code, errmsg = ShellUtil.exec_shell_with_status(exec_cmd, self._logger)

        if code:
            self._logger.info(f"{self} execute hive command: {exec_cmd} success!")
            return True

        self._logger.error(f"{self} execute hive command:{exec_cmd} failed:{errmsg}")
        raise Exception(errmsg)

    def _delete_with_skip_trash(self, hdfs_path):
        shell_cmd = f"hadoop fs -rm -r -skipTrash {hdfs_path}"
        code, msg = ShellUtil.exec_shell_with_status(shell_cmd, self._logger)

        if code:
            self._logger.info(f"{self.action_prefix}{self} remove hdfs path {hdfs_path} success!")
            return True

        self._logger.error(f"{self.action_prefix}{self} remove hdfs path {hdfs_path} failed: {msg}")
        return False

