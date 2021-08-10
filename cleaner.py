#!/usr/bin/env python
# -*- coding-utf8 -*-
"""

:File Name: cleaner
:Author: xufeng
:Date: 2021-08-06 5:40 PM
:Version: v.1.0
:Description:
"""
import traceback
from logging import Logger

from common.logger_adaptor import LogAdaptor
from common.utils import ExpireTimeDesc
from component.hbase_table_cleaner import HbaseTableCleaner
from component.hdfs_path_cleaner import HDFSPathCleaner
from component.hive_table_cleaner import HiveTableCleaner
from component.local_data_cleaner import LocalDataCleaner


class ProjectCleanerBuilder:

    def __init__(self, logger: Logger = None):
        self._logger = LogAdaptor(logger)

        self._cleaners = []

    def build(self):
        # check cleaner exists
        if not self._cleaners:
            raise Exception("no cleaner found!")

        return ProjectDataLogCleaner(self._logger, self._cleaners)

    def with_local_paths(self,
                         local_paths,
                         delete_all_file=False,
                         ignore_delete_type=False,
                         default_suffix=None,
                         ignore_update_time=False,
                         expire_time: ExpireTimeDesc = None):
        """
        :param local_paths: ['/opt/app/logs', 'opt/app/tmp/'] or '/opt/app/xxx.log'
        :param delete_all_file: if delete all file is true,
            file will delete with out check file type and file update time
        :param ignore_delete_type: if set true, file suffix will not check
        :param default_suffix: if ignore_delete_type set true, this param will ignored
            default file suffix
            ex: .log, .out , if set .log, xxx.log.202106 also can be delete
        :param ignore_update_time: if set true, file update time will not check
        :param expire_time: if ignore_update_time set true, this param will ignored
        """
        local_cleaner = LocalDataCleaner(
            self._logger,
            local_paths,
            delete_all_file,
            ignore_delete_type,
            default_suffix,
            ignore_update_time,
            expire_time)
        self._cleaners.append(local_cleaner)
        return self

    def with_log_paths(self,
                       local_paths,
                       delete_all_file=False,
                       ignore_delete_type=False,
                       default_suffix=None,
                       ignore_update_time=False,
                       expire_time: ExpireTimeDesc = None
                       ):
        default_suffix = ['.log', '.out'] if not default_suffix else default_suffix
        return self.with_local_paths(
            local_paths,
            delete_all_file=delete_all_file,
            ignore_delete_type=ignore_delete_type,
            default_suffix=default_suffix,
            ignore_update_time=ignore_update_time,
            expire_time=expire_time)

    def with_hdfs_dirs(self,
                       hdfs_paths,
                       skip_trash=False,
                       ignore_update_time=False,
                       hdfs_day_time_pattern=r'\d{4}-\d{2}-\d{2}',
                       expire_time: ExpireTimeDesc = None):
        """
        delete hdfs path util
        :param hdfs_paths: ['/user/proj/2021/input', 'user/proj/*/tmp' ...]
            if '*' in path, will first use `hadoop fs -ls ...` to list all paths
        :param skip_trash: skip trash flag to remove hadoop file
        :param ignore_update_time: if set true, will delete hadoop files with out check update time
        :param hdfs_day_time_pattern: the day time pattern in hdfs ls line
            ex: drwxr-xr-x   - proj hive          0 2021-07-21 18:29 /user/hive/warehouse/proj.db/tb1 => 2021-07-21
        :param expire_time: if ignore_update_time set true, this param will ignored, default 4 month
        """
        hdfs_cleaner = HDFSPathCleaner(
            self._logger,
            hdfs_paths,
            skip_trash,
            ignore_update_time,
            hdfs_day_time_pattern,
            expire_time
        )
        self._cleaners.append(hdfs_cleaner)
        return self

    def with_hive_tables(self,
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
        hive_table_cleaner = HiveTableCleaner(
            self._logger,
            spark,
            hive_db_name,
            hive_db_warehouse_path,
            hive_cmd,
            is_inner_table,
            skip_trash,
            clear_tables,
            ignore_update_time,
            check_time_type,
            partition_field_format,
            hdfs_day_time_pattern,
            expire_time
        )
        self._cleaners.append(hive_table_cleaner)
        return self

    def with_hbase_tables(self,
                          hbase_namespace,
                          hbase_data_dir='/hbase/data',
                          clear_tables=None,
                          drop_table: bool = False,
                          ignore_update_time: bool = False,
                          hdfs_day_time_pattern=r'\d{4}-\d{2}-\d{2}',
                          expire_time: ExpireTimeDesc = None):
        """
        hbase table cleaner
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
        hbase_table_cleaner = HbaseTableCleaner(
            self._logger,
            hbase_namespace,
            hbase_data_dir,
            clear_tables,
            drop_table,
            ignore_update_time,
            hdfs_day_time_pattern,
            expire_time
        )
        self._cleaners.append(hbase_table_cleaner)
        return self

    def with_es_indexes(self):
        return self


class ProjectDataLogCleaner:

    def __init__(self, logger: LogAdaptor, cleaner):
        self.logger = logger
        self._cleaners = cleaner

    def clean(self):
        """
        clean project data
        :return:
        """
        for cleaner in self._cleaners:
            try:
                self.logger.info(f"begin execute cleaner: {cleaner}")
                cleaner.clean()
                self.logger.info(f"execute cleaner {cleaner} success")
            except Exception:
                self.logger.error(f"execute cleaner {cleaner} failed: {traceback.format_exc()}")

    def test_clean(self):
        """
        show all cleaned data
        :return:
        """
        for cleaner in self._cleaners:
            try:
                self.logger.info(f"begin test cleaner: {cleaner}")
                cleaner.test_clean()
                self.logger.info(f"cleaner {cleaner} test success")
            except Exception:
                self.logger.error(f"cleaner {cleaner} test failed: {traceback.format_exc()}")

    def set_action_prefix(self, prefix):
        """
        set the action log prefix, default `=====`
        :param prefix:
        :return:
        """
        for cleaner in self._cleaners:
            cleaner.set_action_log_prefix(prefix)
