# data_cleaner

### 功能介绍

#### 清理本地日志
    清理指定路径下的日志，可指定多个路径
    指定了后缀的，则只清理指定后缀的日志
    默认日志时间为 15 天，默认不忽略变更时间，不忽略变更时间的，只清理变更时间之前的日志

#### 清理hdfs目录
    清理指定目录下的数据，目录的任何一级可为通配符 *，可指定多个目录
    默认数据过期时间为４个月，默认不忽略变更时间，不忽略变更时间的，只清理变更时间之前的目录
    注意: 目录的变更时间只看指定的最外层目录的变更时间，不遍历子目录

#### 清理hive数据表
    清理指定数据库下的hive表，hive表只有一个通配符的将清理全库，含有字符加通配符的将根据字符和通配符清理对应表
    默认数据过期时间为４个月，默认不忽略变更时间，不忽略变更时间的，只清理变更时间最前的数据
    没有分区的表，会直接根据hive目录更新时间判断是保留全表还是直接删除表
    有分区的表，如果分区是时间格式，清理时间会根据分区判断，不是时间格式的，就按照目录更新时间判断
    默认不清理 .trash 数据，要在清理时彻底删除可手动开启　.trash
    注意: 忽略变更时间的将直接删除hive表

#### 清理hbase数据表
    清理指定命名空间下的hbase表，通配符处理情况同hive
    清理时间根据hbase 的　data 时间来判断
    默认只disable而不drop表，可手动开启drop
    默认数据过期时间为４个月，默认不忽略变更时间，不忽略变更时间的，只清理变更时间最前的数据

#### 清理 es index (暂未实现)

### 使用示例

```python
from cleaner import ProjectCleanerBuilder


pc = ProjectCleanerBuilder() \
    .with_local_paths(local_paths='your_local_dir',
                      delete_all_file=False,
                      default_suffix=['.log', '.out'],
                      ignore_update_time=True
                      ) \
    .build()
    
# 使用前可以验证要删除的数据是否是自己希望删除的数据一致，数据一致的再修改为实际的执行
# 注意，调用　test_clean 后直接调用　clean 是没有效果的，验证　test_clean 没问题后，需注释掉，直接调用 clean 
pc.test_clean()

# 正式使用中调用　clean 执行删除
pc.clean()

```