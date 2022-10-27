package org.apache.hadoop.hbase.client;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FlinkBufferedMutatorImpl extends BufferedMutatorImpl {
    private byte[] familyFiled;
    private byte[] qualifierFiled;

    FlinkBufferedMutatorImpl(
            ClusterConnection conn,
            RpcRetryingCallerFactory rpcCallerFactory,
            RpcControllerFactory rpcFactory,
            BufferedMutatorParams params) {
        super(conn, rpcCallerFactory, rpcFactory, params);
    }

    @Override
    QueueRowAccess createQueueRowAccess() {
        return new FlinkQueueRowAccess();
    }

    public FlinkBufferedMutatorImpl setPreCombineField(byte[] familyFiled, byte[] qualifierFiled) {
        this.familyFiled = familyFiled;
        this.qualifierFiled = qualifierFiled;
        return this;
    }

    class FlinkQueueRowAccess extends QueueRowAccess {
        FlinkQueueRowAccess() {
            super();
            preCombineMutator();
        }

        private byte[] getCell(Mutation m) {
            List<Cell> cells = m.getCellList(familyFiled);
            if (cells != null) {
                Cell cell = cells.stream()
                        .filter(c -> Bytes.equals(c.getQualifier(), qualifierFiled))
                        .findAny().orElse(null);
                if (cell != null) {
                    return  cell.getValue();
                }
            }
            throw new FlinkRuntimeException("the value of precombine filed("
                    + Bytes.toString(familyFiled) + ":" + Bytes.toString(qualifierFiled)
                    + ") is null");
        }

        private void preCombineMutator() {
            //merge writeAsyncBuffer and update size
            Map<byte[], List<Mutation>> allBuffer = writeAsyncBuffer.stream().collect(Collectors.groupingBy(Mutation::getRow));
            AtomicLong writeBufferSize = new AtomicLong(0L);
            List<Mutation> mergedBuffer = allBuffer
                    .values()
                    .stream()
                    .map(list -> list.stream().reduce((m1, m2) -> {
                                byte[] v1 = getCell(m1);
                                byte[] v2 = getCell(m2);
                                return Bytes.compareTo(v1, v2) > 0 ? m1 : m2;
                            }
                    ).orElse(null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            writeAsyncBuffer.clear();
            writeAsyncBuffer.addAll(mergedBuffer);
            currentWriteBufferSize.set(mergedBuffer.stream().mapToLong(Mutation::heapSize).sum());
        }
    }
}
