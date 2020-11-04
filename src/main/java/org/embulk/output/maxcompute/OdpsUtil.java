package org.embulk.output.maxcompute;

import com.aliyun.odps.*;
import com.aliyun.odps.task.SQLTask;
import org.apache.commons.lang3.StringUtils;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class OdpsUtil {

    private static final Logger log = Exec.getLogger(OdpsUtil.class);
    public static int MAX_RETRY_TIME = 3;

    /**
     * Check target table exists in target ODPS project or not
     *
     * @param odps        odps client
     * @param projectName target project name
     * @param tableName   target table name
     * @return boolean
     * @throws OdpsException
     */
    public static boolean isTableExist(Odps odps, String projectName, String tableName) throws OdpsException {
        return odps.tables().exists(projectName, tableName);
    }

    /**
     * Check target table is partition table or not
     *
     * @param odps        odps client
     * @param projectName target project name
     * @param tableName   target table name
     * @return boolean
     */
    public static boolean isPartitionTable(Odps odps, String projectName, String tableName) {
        Table table = odps.tables().get(projectName, tableName);
        List<Column> partitionKeys = table.getSchema().getPartitionColumns();
        return null != partitionKeys && !partitionKeys.isEmpty();
    }

    /**
     * List all the existing ODPS table partition
     *
     * @param table taget odps table
     * @return list of partitions
     * @throws OdpsException
     */
    public static List<String> listOdpsPartitions(Table table) throws OdpsException {
        List<String> parts = new ArrayList<String>();
        try {
            List<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                parts.add(partition.getPartitionSpec().toString());
            }
        } catch (Exception e) {
            throw new OdpsException(e);
        }
        return parts;
    }

    /**
     * Check target partition exists or not
     *
     * @param table     target odps table
     * @param partition target table partition spec
     * @return boolean
     * @throws OdpsException
     */
    private static boolean isPartitionExist(Table table, String partition) throws OdpsException {
        List<String> odpsParts = OdpsUtil.listOdpsPartitions(table);

        int j = 0;
        for (; j < odpsParts.size(); j++) {
            if (odpsParts.get(j).replaceAll("'", "").equals(partition)) {
                break;
            }
        }

        return j != odpsParts.size();
    }

    /**
     * Prepare partition information of target table
     * Add new partition if target partition does not exist
     *
     * @param odps        odps client
     * @param projectName target project name
     * @param tableName   target table name
     * @param partition   target table partition spec
     * @throws OdpsException
     */
    public static void preparePartition(Odps odps, String projectName, String tableName, String partition) throws OdpsException {
        Table table = odps.tables().get(projectName, tableName);
        if (OdpsUtil.isPartitionExist(table, partition)) {
            log.info(String.format("The target partition [%s] exists, no need to add new one!", partition));
        } else {
            log.info(String.format("Add target partition [%s] with table [%s] of project [%s]!", partition, tableName, projectName));
            StringBuilder addPart = new StringBuilder();
            addPart.append("alter table ").append(table.getName()).append(" add IF NOT EXISTS partition(")
                    .append(partition).append(");");
            try {
                runSqlTaskWithRetry(odps, addPart.toString(), MAX_RETRY_TIME, 1000, true);
            } catch (Exception e) {
                log.error(String.format("Failed to add partition [%s] for table [%s] of project [%s]", partition, tableName, projectName));
                throw new OdpsException(e);
            }
        }
    }

    /**
     * Truncate non-partition table
     * @param odps odps client
     * @param projectName target project name
     * @param tableName target table name
     * @throws OdpsException
     */
    public static void truncateNonePartitionTable(Odps odps, String projectName, String tableName) throws OdpsException {
        log.info(String.format("Truncate non-partition table [%s] of project [%s]", tableName, projectName));
        String truncateNonPartitionedTableSql = "truncate table " + tableName + ";";
        try {
            runSqlTaskWithRetry(odps, truncateNonPartitionedTableSql, MAX_RETRY_TIME, 1000, true);
        } catch (Exception e) {
            log.error(String.format("Failed to truncate non-partition table [%s] of project [%s]", tableName, projectName));
            throw new OdpsException(e);
        }
    }

    /**
     * Drop target partition
     * @param odps odps client
     * @param projectName target project name
     * @param tableName target table name
     * @param partition target partition
     * @throws OdpsException
     */
    public static void dropPartition(Odps odps, String projectName, String tableName, String partition) throws OdpsException {
        Table table = odps.tables().get(projectName, tableName);
        if (OdpsUtil.isPartitionExist(table, partition)) {
            log.info(String.format("The target partition [%s] exists! Running drop partition action!", partition));
            StringBuilder dropPart = new StringBuilder();
            dropPart.append("alter table ").append(table.getName())
                    .append(" drop IF EXISTS partition(").append(partition)
                    .append(");");
            try {
                runSqlTaskWithRetry(odps, dropPart.toString(), MAX_RETRY_TIME, 1000, true);
            } catch (Exception e) {
                log.error(String.format("Failed to drop partition [%s] with table [%s] of project [%s]", partition, tableName, projectName));
                throw new OdpsException(e);
            }
        } else {
            log.info(String.format("The target partition [%s] does not exist! No need to run drop partition action!", partition));
        }
    }

    /**
     * Run ODPS SQL task with retry strategy
     *
     * @param odps                   odps client
     * @param query                  target sql query
     * @param retryTimes             retry times of strategy
     * @param sleepTimeInMilliSecond sleep time of strategy
     * @param exponential
     * @throws Exception
     */
    public static void runSqlTaskWithRetry(final Odps odps, final String query, int retryTimes,
                                           long sleepTimeInMilliSecond, boolean exponential) throws Exception {
        for (int i = 0; i < retryTimes; i++) {
            try {
                runSqlTask(odps, query);
                return;
            } catch (OdpsTaskException e) {
                log.debug("Exception when calling callable", e);
                if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                    log.warn(String.format("will do [%s] times retry, current exception=%s", i + 1, e.getMessage()));
                    long timeToSleep;
                    if (exponential) {
                        timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                    } else {
                        timeToSleep = sleepTimeInMilliSecond;
                    }
                    if (timeToSleep >= 128 * 1000) {
                        timeToSleep = 128 * 1000;
                    }

                    try {
                        Thread.sleep(timeToSleep);
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    throw e;
                }

            } catch (Exception e) {
                throw e;
            }
        }
    }

    /**
     * Run Odps SQL task
     *
     * @param odps  odps client
     * @param query target sql query
     * @throws OdpsTaskException
     */
    public static void runSqlTask(Odps odps, String query) throws OdpsTaskException {
        if (StringUtils.isBlank(query)) {
            return;
        }

        String taskName = "odps_task_" + UUID.randomUUID().toString().replace('-', '_');

        log.info(String.format("Try to start sqlTask:[%s] to run odps sql:[\n%s\n] .", taskName, query));

        Instance instance;
        Instance.TaskStatus status;
        try {
            instance = SQLTask.run(odps, odps.getDefaultProject(), query, taskName, null, null);
            instance.waitForSuccess();
            status = instance.getTaskStatus().get(taskName);
            if (!Instance.TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
                throw new OdpsTaskException(String.format("Failed to run ODPS SQL task with status : [%s] and sql : [\n%s\n]", status, query));
            }
        } catch (OdpsTaskException e) {
            throw e;
        } catch (Exception e) {
            throw new OdpsTaskException(String.format("Failed to run ODPS SQL task with sql : [\n%s\n] and related exception message: [\n%s\n]", query, e.getMessage()));
        }
    }
}
