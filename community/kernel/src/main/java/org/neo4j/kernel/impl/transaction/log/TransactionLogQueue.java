/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscUnboundedXaddArrayQueue;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionLogQueue extends LifecycleAdapter {
    private static final int CONSUMER_MAX_BATCH = 1024;
    private static final int INITIAL_CAPACITY = 128;
    private static final int FAILED_TX_MARKER = -1;

    private final LogFiles logFiles;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final Panic databasePanic;
    private final MpscUnboundedXaddArrayQueue<TxQueueElement> txAppendQueue;
    private final JobScheduler jobScheduler;
    private final InternalLog log;
    private TransactionWriter transactionWriter;
    private Thread logAppender;
    private volatile boolean stopped;

    public TransactionLogQueue(
            LogFiles logFiles,
            TransactionIdStore transactionIdStore,
            Panic databasePanic,
            JobScheduler jobScheduler,
            InternalLogProvider logProvider) {
        this.logFiles = logFiles;
        this.logRotation = logFiles.getLogFile().getLogRotation();
        this.transactionIdStore = transactionIdStore;
        this.databasePanic = databasePanic;
        this.txAppendQueue = new MpscUnboundedXaddArrayQueue<>(INITIAL_CAPACITY);
        this.jobScheduler = jobScheduler;
        this.stopped = true;
        this.log = logProvider.getLog(getClass());
    }

    public TxQueueElement submit(CommandBatchToApply batch, LogAppendEvent logAppendEvent) throws IOException {
        if (stopped) {
            throw new DatabaseShutdownException();
        }
        TxQueueElement txQueueElement = new TxQueueElement(batch, logAppendEvent);
        while (!txAppendQueue.offer(txQueueElement)) {
            if (stopped) {
                throw new DatabaseShutdownException();
            }
        }
        LockSupport.unpark(logAppender);
        return txQueueElement;
    }

    @Override
    public synchronized void start() {
        transactionWriter = new TransactionWriter(
                txAppendQueue, logFiles.getLogFile(), transactionIdStore, databasePanic, logRotation, log);
        logAppender = jobScheduler.threadFactory(Group.LOG_WRITER).newThread(transactionWriter);
        logAppender.start();
        stopped = false;
    }

    @Override
    public synchronized void shutdown() throws ExecutionException, InterruptedException {
        stopped = true;
        TransactionWriter writer = this.transactionWriter;
        if (writer != null) {
            writer.stop();
        }

        Thread appender = this.logAppender;
        if (appender != null) {
            appender.join();
        }
    }

    static class TxQueueElement {
        private static final long PARK_TIME = MILLISECONDS.toNanos(100);

        private final CommandBatchToApply batch;
        private final LogAppendEvent logAppendEvent;
        private final Thread executor;
        private Throwable throwable;
        private TxQueueElement[] elementsToNotify;
        private volatile long[] txIds;
        private volatile long txId;

        TxQueueElement(CommandBatchToApply batch, LogAppendEvent logAppendEvent) {
            this.batch = batch;
            this.logAppendEvent = logAppendEvent;
            this.executor = Thread.currentThread();
        }

        public long getCommittedTxId() {
            while (txId == 0 && txIds == null) {
                LockSupport.parkNanos(PARK_TIME);
            }
            var elements = this.elementsToNotify;
            if (elements != null) {
                long[] ids = txIds;
                for (int i = 1; i < elements.length; i++) {
                    TxQueueElement element = elements[i];
                    element.txId = ids[i];
                    LockSupport.unpark(element.executor);
                }
                txId = ids[0];
            }
            var exception = throwable;
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            return txId;
        }

        public void fail(Throwable throwable) {
            this.throwable = throwable;
            this.txId = FAILED_TX_MARKER;
            LockSupport.unpark(executor);
        }
    }

    private static class TransactionWriter implements Runnable {
        private final MpscUnboundedXaddArrayQueue<TxQueueElement> txQueue;
        private final TransactionLogWriter transactionLogWriter;
        private final LogFile logFile;
        private final Panic databasePanic;
        private final LogRotation logRotation;
        private final InternalLog log;
        private final int checksum;
        private volatile boolean stopped;
        private final MessagePassingQueue.WaitStrategy waitStrategy;

        TransactionWriter(
                MpscUnboundedXaddArrayQueue<TxQueueElement> txQueue,
                LogFile logFile,
                TransactionIdStore transactionIdStore,
                Panic databasePanic,
                LogRotation logRotation,
                InternalLog log) {
            this.txQueue = txQueue;
            this.transactionLogWriter = logFile.getTransactionLogWriter();
            this.logFile = logFile;
            this.checksum = transactionIdStore.getLastCommittedTransaction().checksum();
            this.databasePanic = databasePanic;
            this.logRotation = logRotation;
            this.log = log;
            this.waitStrategy = new SpinParkCombineWaitingStrategy();
        }

        @Override
        public void run() {
            TxConsumer txConsumer = new TxConsumer(databasePanic, transactionLogWriter, checksum);

            int idleCounter = 0;
            while (!stopped) {
                try {
                    int drainedElements = txQueue.drain(txConsumer, CONSUMER_MAX_BATCH);
                    if (drainedElements > 0) {
                        idleCounter = 0;
                        txConsumer.processBatch();

                        LogAppendEvent logAppendEvent = txConsumer.txElements[drainedElements - 1].logAppendEvent;
                        boolean logRotated = logRotation.locklessRotateLogIfNeeded(logAppendEvent);
                        logAppendEvent.setLogRotated(logRotated);
                        if (!logRotated) {
                            logFile.locklessForce(logAppendEvent);
                        }
                        txConsumer.complete();
                    } else {
                        idleCounter = waitStrategy.idle(idleCounter);
                    }
                } catch (Throwable t) {
                    log.error("Transaction log applier failure.", t);
                    databasePanic.panic(t);
                    txConsumer.cancelBatch(t);
                }
            }

            DatabaseShutdownException databaseShutdownException = new DatabaseShutdownException();
            TxQueueElement element;
            while ((element = txQueue.poll()) != null) {
                element.fail(databaseShutdownException);
            }
        }

        private static class TxConsumer implements MessagePassingQueue.Consumer<TxQueueElement> {
            private final Panic databasePanic;
            private final TransactionLogWriter transactionLogWriter;

            private int checksum;
            private final TxQueueElement[] txElements = new TransactionLogQueue.TxQueueElement[CONSUMER_MAX_BATCH];
            private int index;
            private TxQueueElement[] elements;
            private long[] txIds;

            TxConsumer(Panic databasePanic, TransactionLogWriter transactionLogWriter, int checksum) {
                this.databasePanic = databasePanic;
                this.transactionLogWriter = transactionLogWriter;
                this.checksum = checksum;
            }

            @Override
            public void accept(TxQueueElement txQueueElement) {
                txElements[index++] = txQueueElement;
            }

            private void processBatch() throws IOException {
                databasePanic.assertNoPanic(IOException.class);
                int drainedElements = index;
                elements = new TxQueueElement[drainedElements];
                txIds = new long[drainedElements];
                for (int i = 0; i < drainedElements; i++) {
                    TxQueueElement txQueueElement = txElements[i];
                    elements[i] = txQueueElement;
                    LogAppendEvent logAppendEvent = txQueueElement.logAppendEvent;
                    long lastTransactionId = TransactionIdStore.BASE_TX_ID;
                    try (var appendEvent = logAppendEvent.beginAppendTransaction(drainedElements)) {
                        CommandBatchToApply commands = txQueueElement.batch;
                        while (commands != null) {
                            long transactionId = commands.transactionId();
                            appendToLog(commands, transactionId, logAppendEvent);
                            commands = commands.next();
                            lastTransactionId = transactionId;
                        }
                        txIds[i] = lastTransactionId;
                    } catch (Exception e) {
                        throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                }
            }

            private void appendToLog(
                    CommandBatchToApply commandBatchToApply, long transactionId, LogAppendEvent logAppendEvent)
                    throws IOException {
                var logPositionBeforeCommit = transactionLogWriter.getCurrentPosition();
                transactionLogWriter.resetAppendedBytesCounter();
                CommandBatch commandBatch = commandBatchToApply.commandBatch();
                this.checksum = transactionLogWriter.append(
                        commandBatch,
                        transactionId,
                        commandBatchToApply.chunkId(),
                        checksum,
                        commandBatchToApply.previousBatchLogPosition());
                var logPositionAfterCommit = transactionLogWriter.getCurrentPosition();
                logAppendEvent.appendedBytes(transactionLogWriter.getAppendedBytes());
                commandBatchToApply.batchAppended(logPositionBeforeCommit, logPositionAfterCommit, checksum);
            }

            public void complete() {
                TxQueueElement first = txElements[0];
                first.elementsToNotify = elements;
                first.txIds = txIds;
                LockSupport.unpark(first.executor);

                Arrays.fill(txElements, 0, index, null);
                index = 0;
            }

            public void cancelBatch(Throwable t) {
                for (int i = 0; i < index; i++) {
                    txElements[i].fail(t);
                }
                Arrays.fill(txElements, 0, index, null);
                index = 0;
            }
        }

        public void stop() {
            stopped = true;
        }
    }

    /**
     * Message wait strategy that will try to wait at first for number of times for new work by using Thread.onSpinWait, and fallback to parkNanos
     * if new work did not arrive.
     * This is a strategy that should be good on systems with lots of work but may cause some increased latency spikes on systems with relatively small
     * number of incoming transactions.
     */
    private static class SpinParkCombineWaitingStrategy implements MessagePassingQueue.WaitStrategy {
        private static final int SPIN_THRESHOLD = Runtime.getRuntime().availableProcessors() < 2 ? 1 : 1000;
        private static final int SHORT_PARK_THRESHOLD = 100_000;
        private static final int LONG_PARK_COUNTER = SHORT_PARK_THRESHOLD + 1;
        private static final int SHORT_PARK_TIME = 10;
        private static final long LONG_PARK_TIME = MILLISECONDS.toNanos(10);

        @Override
        public int idle(int idleCounter) {
            if (idleCounter < SPIN_THRESHOLD) {
                Thread.onSpinWait();
            } else if (idleCounter < SHORT_PARK_THRESHOLD) {
                parkNanos(SHORT_PARK_TIME);
            } else {
                parkNanos(LONG_PARK_TIME);
                return LONG_PARK_COUNTER;
            }
            return idleCounter + 1;
        }
    }
}
