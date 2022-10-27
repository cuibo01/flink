package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FlinkConnectionImplementation extends ConnectionManager.HConnectionImplementation {
    FlinkConnectionImplementation(Configuration conf, boolean managed) throws IOException {
        super(conf, managed);
    }

    public FlinkConnectionImplementation(
            Configuration conf,
            boolean managed,
            ExecutorService pool,
            User user) throws IOException {
        super(conf, managed, pool, user);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) {
        if (params.getTableName() == null) {
            throw new IllegalArgumentException("TableName cannot be null.");
        } else {
            if (params.getPool() == null) {
                params.pool(HTable.getDefaultExecutor(this.getConfiguration()));
            }

            if (params.getWriteBufferSize() == -1L) {
                params.writeBufferSize(getConnectionConfiguration().getWriteBufferSize());
            }

            if (params.getMaxKeyValueSize() == -1) {
                params.maxKeyValueSize(this.getConnectionConfiguration().getMaxKeyValueSize());
            }

            return new FlinkBufferedMutatorImpl(this, this.getRpcRetryingCallerFactory(),
                    this.getRpcControllerFactory(), params);
        }
    }
}
