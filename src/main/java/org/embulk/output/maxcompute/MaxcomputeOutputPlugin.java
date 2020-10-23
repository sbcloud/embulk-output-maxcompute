package org.embulk.output.maxcompute;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class MaxcomputeOutputPlugin
        implements OutputPlugin {
    public interface PluginTask
            extends Task {
        @Config("accessKeyId")
        public String getAccessKeyID();

        @Config("accessKeySecret")
        public String getAccessKeySecret();

        @Config("odpsUrl")
        @ConfigDefault("\"http://service.ap-northeast-1.maxcompute.aliyun.com/api\"")
        public Optional<String> getOdpsUrl();

        @Config("tunnelUrl")
        @ConfigDefault("\"http://dt.ap-northeast-1.maxcompute.aliyun.com\"")
        public Optional<String> getTunnelUrl();

        @Config("projectName")
        public String getProjectName();

        @Config("tableName")
        public String getTableName();

        @Config("partition")
        @ConfigDefault("null")
        Optional<String> getPartition();
    }

    public class MaxcomputePageOutput implements TransactionalPageOutput {

        private final Logger log = Exec.getLogger(MaxcomputePageOutput.class);

        private final DateTimeFormatter TO_STRING_FORMATTER_MILLIS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ").withZoneUTC();
        private PageReader pageReader;
        private PluginTask task;
        private Schema schema;
        private TunnelBufferedWriter recordWriter;
        private Odps odps;
        private TableTunnel.UploadSession uploadSession;
        private RetryStrategy retryStrategy;


        public MaxcomputePageOutput(PluginTask task, Schema schema) {
            this.pageReader = new PageReader(schema);
            this.task = task;
            this.schema = schema;
            this.odps = generateOdpsClient(task);
            this.uploadSession = generateTableUploadSession(odps, task);
            this.retryStrategy = new RetryStrategy(6, 4, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
        }

        private Odps generateOdpsClient(PluginTask task) {
            if (task.getOdpsUrl().isPresent() && !task.getAccessKeyID().isEmpty() && !task.getAccessKeySecret().isEmpty() && !task.getProjectName().isEmpty()) {
                Account account = new AliyunAccount(task.getAccessKeyID(), task.getAccessKeySecret());
                Odps odps = new Odps(account);
                odps.setEndpoint(task.getOdpsUrl().get());
                odps.setDefaultProject(task.getProjectName());
                return odps;
            } else {
                throw new UnsupportedOperationException("Less parameters for maxcompute output plugin to create ODPS client");
            }
        }

        private TableTunnel.UploadSession generateTableUploadSession(Odps odps, PluginTask task) {
            if (null != odps && task.getTunnelUrl().isPresent()) {
                try {
                    TableTunnel tableTunnel = new TableTunnel(odps);
                    TableTunnel.UploadSession uploadSession;
                    if (!task.getPartition().isPresent()) {
                        log.info("Running with no partition mode");
                        uploadSession = tableTunnel.createUploadSession(task.getProjectName(), task.getTableName());
                    } else {
                        log.info("Running with partition mode as : " + task.getPartition().get());
                        PartitionSpec partitionSpec = new PartitionSpec(task.getPartition().get());
                        uploadSession = tableTunnel.createUploadSession(task.getProjectName(), task.getTableName(), partitionSpec);
                    }
                    return uploadSession;
                } catch (TunnelException e) {
                    log.error(e.getErrorMsg());
                    throw new UnsupportedOperationException("Failed to create table tunnel session");
                }
            } else {
                throw new UnsupportedOperationException("Less parameters for maxcompute output plugin to create table tunnel session");
            }
        }

        @Override
        public void add(Page page) {
            pageReader.setPage(page);

            try {
                recordWriter = (TunnelBufferedWriter) uploadSession.openBufferedWriter();
                recordWriter.setRetryStrategy(retryStrategy);
                Record tmp = uploadSession.newRecord();
                while (pageReader.nextRecord()) {
                    for (Column column : schema.getColumns()) {
                        // Data Format https://github.com/alibaba/DataX/blob/master/odpswriter/doc/odpswriter.md
                        if (column.getType() instanceof StringType) {
                            tmp.setString(column.getName(), pageReader.getString(column));
                        } else if (column.getType() instanceof BooleanType) {
                            tmp.setBoolean(column.getName(), pageReader.getBoolean(column));
                        } else if (column.getType() instanceof LongType) {
                            tmp.setBigint(column.getName(), pageReader.getLong(column));
                        } else if (column.getType() instanceof DoubleType) {
                            tmp.setDouble(column.getName(), pageReader.getDouble(column));
                        } else if (column.getType() instanceof TimestampType) {
                            Timestamp timestamp = pageReader.getTimestamp(column);
                            tmp.setDatetime(column.getName(), new Date(timestamp.toEpochMilli()));
                        }
                    }
                    recordWriter.write(tmp);
                }
                recordWriter.close();
                uploadSession.commit();
            } catch (TunnelException | IOException e) {
                throw new UnsupportedOperationException("Failed to upload related data");
            }
        }

        @Override
        public void finish() {

        }

        @Override
        public void close() {

        }

        @Override
        public void abort() {

        }

        @Override
        public TaskReport commit() {
            return Exec.newTaskReport();
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control) {
        throw new UnsupportedOperationException("maxcompute output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        MaxcomputePageOutput pageOutput = new MaxcomputePageOutput(task, schema);
        return pageOutput;
    }
}
