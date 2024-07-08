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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.kernel.impl.transaction.log.checkpoint.LatestCheckpointInfo.UNKNOWN_CHECKPOINT_INFO;

import java.io.IOException;
import java.time.Clock;
import java.util.function.BooleanSupplier;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.ClosedBatchMetadata;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.time.Stopwatch;

public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer {
    private static final String CHECKPOINT_TAG = "checkpoint";
    private static final long NO_TRANSACTION_ID = -1;
    private static final String IO_DETAILS_TEMPLATE =
            "Checkpoint flushed %d pages (%d%% of total available pages), in %d IOs. Checkpoint performed with IO limit: %s, paused in total %d times( %d millis).";
    private static final String UNLIMITED_IO_CONTROLLER_LIMIT = "unlimited";

    private final CheckpointAppender checkpointAppender;
    private final MetadataProvider metadataProvider;
    private final CheckPointThreshold threshold;
    private final ForceOperation forceOperation;
    private final LogPruning logPruning;
    private final Panic databasePanic;
    private final InternalLog log;
    private final DatabaseTracers tracers;
    private final StoreCopyCheckPointMutex mutex;
    private final CursorContextFactory cursorContextFactory;
    private final Clock clock;
    private final IOController ioController;
    private final KernelVersionProvider versionProvider;

    private volatile boolean shutdown;
    private volatile LatestCheckpointInfo latestCheckPointInfo = UNKNOWN_CHECKPOINT_INFO;

    public CheckPointerImpl(
            MetadataProvider metadataProvider,
            CheckPointThreshold threshold,
            ForceOperation forceOperation,
            LogPruning logPruning,
            CheckpointAppender checkpointAppender,
            Panic databasePanic,
            InternalLogProvider logProvider,
            DatabaseTracers tracers,
            StoreCopyCheckPointMutex mutex,
            CursorContextFactory cursorContextFactory,
            Clock clock,
            IOController ioController,
            KernelVersionProvider versionProvider) {
        this.checkpointAppender = checkpointAppender;
        this.metadataProvider = metadataProvider;
        this.threshold = threshold;
        this.forceOperation = forceOperation;
        this.logPruning = logPruning;
        this.databasePanic = databasePanic;
        this.log = logProvider.getLog(CheckPointerImpl.class);
        this.tracers = tracers;
        this.mutex = mutex;
        this.cursorContextFactory = cursorContextFactory;
        this.clock = clock;
        this.ioController = ioController;
        this.versionProvider = versionProvider;
    }

    @Override
    public void start() {
        var lastClosedTransaction = metadataProvider.getLastClosedTransaction();
        threshold.initialize(metadataProvider.getLastAppendIndex(), lastClosedTransaction.logPosition());
    }

    @Override
    public void shutdown() {
        try (var ignored = mutex.checkPoint()) {
            shutdown = true;
        }
    }

    @Override
    public long forceCheckPoint(TriggerInfo info) throws IOException {
        try (Resource lock = mutex.checkPoint()) {
            return checkpointByTrigger(info);
        }
    }

    @Override
    public long forceCheckPoint(
            TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo)
            throws IOException {
        try (Resource lock = mutex.checkPoint()) {
            return checkpointByExternalParams(transactionId, position, position, appendIndex, triggerInfo);
        }
    }

    @Override
    public long tryCheckPoint(TriggerInfo info) throws IOException {
        return tryCheckPoint(info, () -> false);
    }

    @Override
    public long tryCheckPointNoWait(TriggerInfo info) throws IOException {
        return tryCheckPoint(info, () -> true);
    }

    @Override
    public long tryCheckPoint(TriggerInfo info, BooleanSupplier timeout) throws IOException {
        Resource lockAttempt = mutex.tryCheckPoint();
        if (lockAttempt != null) {
            try (lockAttempt) {
                return checkpointByTrigger(info);
            }
        } else {
            try (Resource lock = mutex.tryCheckPoint(timeout)) {
                if (lock != null) {
                    var lastInfo = latestCheckPointInfo;
                    log.info(info.describe(lastInfo) + " Check pointing was already running, completed now");
                    return lastInfo.checkpointedTransactionId().id();
                } else {
                    return NO_TRANSACTION_ID;
                }
            }
        }
    }

    @Override
    public long checkPointIfNeeded(TriggerInfo info) throws IOException {
        var lastClosedTransaction = metadataProvider.getLastClosedTransaction();
        if (threshold.isCheckPointingNeeded(
                lastClosedTransaction.transactionId().appendIndex(), lastClosedTransaction.logPosition(), info)) {
            try (Resource lock = mutex.checkPoint()) {
                return checkpointByTrigger(info);
            }
        }
        return NO_TRANSACTION_ID;
    }

    private long checkpointByTrigger(TriggerInfo triggerInfo) throws IOException {
        if (shutdown) {
            logShutdownMessage(triggerInfo);
            return NO_TRANSACTION_ID;
        }
        var lastClosedTransaction = metadataProvider.getLastClosedTransaction().transactionId();
        var lastClosedBatch = metadataProvider.getLastClosedBatch();
        var oldestNotVisibleTransaction = evaluateOldestNotVisibleTransactionStartPosition(lastClosedBatch);

        return checkpointByExternalParams(
                lastClosedTransaction,
                oldestNotVisibleTransaction,
                lastClosedBatch.logPosition(),
                lastClosedBatch.appendIndex(),
                triggerInfo);
    }

    private long checkpointByExternalParams(
            TransactionId transactionId,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            long appendIndex,
            TriggerInfo triggerInfo)
            throws IOException {
        if (shutdown) {
            logShutdownMessage(triggerInfo);
            return NO_TRANSACTION_ID;
        }
        return doCheckpoint(
                transactionId, appendIndex, oldestNotCompletedPosition, checkpointedLogPosition, triggerInfo);
    }

    private long doCheckpoint(
            TransactionId transactionId,
            long appendIndex,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            TriggerInfo triggerInfo)
            throws IOException {
        var databaseTracer = tracers.getDatabaseTracer();
        try (var cursorContext = cursorContextFactory.create(CHECKPOINT_TAG);
                LogCheckPointEvent checkPointEvent = databaseTracer.beginCheckPoint()) {
            long lastClosedTransactionId = transactionId.id();
            cursorContext.getVersionContext().initWrite(lastClosedTransactionId);
            KernelVersion kernelVersion = versionProvider.kernelVersion();
            var ongoingCheckpoint = new LatestCheckpointInfo(transactionId, appendIndex);
            String checkpointReason = triggerInfo.describe(ongoingCheckpoint);
            /*
             * Check kernel health before going into waiting for transactions to be closed, to avoid
             * getting into a scenario where we would await a condition that would potentially never
             * happen.
             */
            databasePanic.assertNoPanic(IOException.class);
            /*
             * First we flush the store. If we fail now or during the flush, on recovery we'll find the
             * earlier check point and replay from there all the log entries. Everything will be ok.
             */
            log.info(checkpointReason + " checkpoint started...");
            Stopwatch startTime = Stopwatch.start();

            try (var flushEvent = checkPointEvent.beginDatabaseFlush()) {
                forceOperation.flushAndForce(flushEvent, cursorContext);
                flushEvent.ioControllerLimit(ioController.configuredLimit());
            }

            /*
             * Check kernel health before going to write the next check point.  In case of a panic this check point
             * will be aborted, which is the safest alternative so that the next recovery will have a chance to
             * repair the damages.
             */
            databasePanic.assertNoPanic(IOException.class);
            checkpointAppender.checkPoint(
                    checkPointEvent,
                    transactionId,
                    appendIndex,
                    kernelVersion,
                    oldestNotCompletedPosition,
                    checkpointedLogPosition,
                    clock.instant(),
                    checkpointReason);
            threshold.checkPointHappened(appendIndex, checkpointedLogPosition);
            long durationMillis = startTime.elapsed(MILLISECONDS);
            checkPointEvent.checkpointCompleted(durationMillis);
            log.info(createCheckpointMessageDescription(checkPointEvent, checkpointReason, durationMillis));

            /*
             * Prune up to the version pointed from the latest check point,
             * since it might be an earlier version than the current log version.
             */
            logPruning.pruneLogs(oldestNotCompletedPosition.getLogVersion());
            latestCheckPointInfo = ongoingCheckpoint;
            return lastClosedTransactionId;
        } catch (Throwable t) {
            // Why only log failure here? It's because check point can potentially be made from various
            // points of execution e.g. background thread triggering check point if needed and during
            // shutdown where it's better to have more control over failure handling.
            log.error("Checkpoint failed", t);
            throw t;
        }
    }

    private String createCheckpointMessageDescription(
            LogCheckPointEvent checkpointEvent, String checkpointReason, long durationMillis) {
        double flushRatio = checkpointEvent.flushRatio();
        long ioLimit = checkpointEvent.getConfiguredIOLimit();
        String ioDetails = IO_DETAILS_TEMPLATE.formatted(
                checkpointEvent.getPagesFlushed(),
                (int) (flushRatio * 100),
                checkpointEvent.getIOsPerformed(),
                ioLimitDescription(ioLimit),
                checkpointEvent.getTimesPaused(),
                checkpointEvent.getMillisPaused());
        return checkpointReason + " checkpoint completed in " + duration(durationMillis) + ". " + ioDetails;
    }

    private String ioLimitDescription(long ioLimit) {
        return ioController.isEnabled() && ioLimit >= 0 ? String.valueOf(ioLimit) : UNLIMITED_IO_CONTROLLER_LIMIT;
    }

    private void logShutdownMessage(TriggerInfo triggerInfo) {
        log.warn("Checkpoint was requested on already shutdown checkpointer. Requester: "
                + triggerInfo.describe(UNKNOWN_CHECKPOINT_INFO));
    }

    private LogPosition evaluateOldestNotVisibleTransactionStartPosition(ClosedBatchMetadata lastClosedBatch) {
        var openTransactionMetadata = metadataProvider.getOldestOpenTransaction();
        if (openTransactionMetadata == null) {
            return lastClosedBatch.logPosition();
        }

        long oldestBatchAppendIndex = openTransactionMetadata.appendIndex();
        // oldest not closed is after closed tx id so nothing to guard
        if (oldestBatchAppendIndex > lastClosedBatch.appendIndex()) {
            return lastClosedBatch.logPosition();
        }

        return openTransactionMetadata.logPosition();
    }

    @Override
    public LatestCheckpointInfo latestCheckPointInfo() {
        return latestCheckPointInfo;
    }

    @FunctionalInterface
    public interface ForceOperation {
        ForceOperation NO_OP = (flushEvent, cursorContext) -> {};

        void flushAndForce(DatabaseFlushEvent flushEvent, CursorContext cursorContext) throws IOException;
    }
}
