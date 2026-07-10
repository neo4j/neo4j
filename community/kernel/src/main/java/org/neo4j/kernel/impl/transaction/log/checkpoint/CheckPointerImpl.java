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
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_async_io;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.kernel.impl.transaction.log.checkpoint.LatestCheckpointInfo.UNKNOWN_CHECKPOINT_INFO;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.async.AsyncIOProvider;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.AsyncCheckpointCompletionHandler;
import org.neo4j.io.pagecache.impl.muninn.AsyncCheckpointFailureHandler;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.ClosedBatchMetadata;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.VisibleForTesting;

public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer {
    private static final String CHECKPOINT_TAG = "checkpoint";
    private static final long NO_APPEND_INDEX = -1;
    private static final String IO_DETAILS_TEMPLATE =
            "Checkpoint flushed %d pages (%d%% of total available pages), in %d IOs. Checkpoint performed with IO limit: %s, paused in total %d times(%d millis). Average checkpoint flush speed: %s.";
    private static final String UNLIMITED_IO_CONTROLLER_LIMIT = "unlimited";

    private final CheckpointFile checkpointFile;
    private final LogMetadataProvider metadataProvider;
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
    private final MemoryTracker memoryTracker;
    private final Config config;

    private volatile boolean shutdown;
    private volatile LatestCheckpointInfo latestCheckPointInfo = UNKNOWN_CHECKPOINT_INFO;

    public CheckPointerImpl(
            LogMetadataProvider metadataProvider,
            CheckPointThreshold threshold,
            ForceOperation forceOperation,
            LogPruning logPruning,
            CheckpointFile checkpointFile,
            Panic databasePanic,
            InternalLogProvider logProvider,
            DatabaseTracers tracers,
            StoreCopyCheckPointMutex mutex,
            CursorContextFactory cursorContextFactory,
            Clock clock,
            IOController ioController,
            MemoryTracker memoryTracker,
            Config config) {
        this.checkpointFile = checkpointFile;
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
        this.memoryTracker = memoryTracker;
        this.config = config;
    }

    @Override
    public void start() {
        var lastClosedBatch = metadataProvider.getLastClosedBatch();
        threshold.initialize(lastClosedBatch.appendIndex(), lastClosedBatch.logPosition());
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
            return checkpointByTrigger(info, false);
        }
    }

    @Override
    public long forceCheckPoint(TriggerInfo info, boolean skipLogPruning) throws IOException {
        try (Resource lock = mutex.checkPoint()) {
            return checkpointByTrigger(info, skipLogPruning);
        }
    }

    @Override
    public long forceCheckPoint(
            TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo)
            throws IOException {
        try (Resource lock = mutex.checkPoint()) {
            return checkpointByExternalParams(
                    transactionId, appendIndex, position, position, appendIndex, triggerInfo, false);
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
                return checkpointByTrigger(info, false);
            }
        } else {
            try (Resource lock = mutex.tryCheckPoint(timeout)) {
                if (lock != null) {
                    var lastInfo = latestCheckPointInfo;
                    log.info(info.describe(lastInfo) + " Check pointing was already running, completed now");
                    return lastInfo.appendIndex();
                } else {
                    return NO_APPEND_INDEX;
                }
            }
        }
    }

    @Override
    public long checkPointIfNeeded(TriggerInfo info) throws IOException {
        var lastClosedBatch = metadataProvider.getLastClosedBatch();
        if (threshold.isCheckPointingNeeded(lastClosedBatch.appendIndex(), lastClosedBatch.logPosition(), info)) {
            try (Resource lock = mutex.checkPoint()) {
                return checkpointByTrigger(info, false);
            }
        }
        return NO_APPEND_INDEX;
    }

    @Override
    public long compact() throws IOException {
        try (var checkpointLock = mutex.checkPoint();
                var cursorContext = cursorContextFactory.create(CHECKPOINT_TAG);
                var checkPointEvent = tracers.getDatabaseTracer().beginCheckPoint();
                var flushEvent = checkPointEvent.beginDatabaseFlush()) {
            return forceOperation.compact(
                    flushEvent,
                    createAsyncBlockAccessor(AsyncIOProvider.getInstance(), memoryTracker, flushEvent),
                    cursorContext);
        }
    }

    private long checkpointByTrigger(TriggerInfo triggerInfo, boolean skipLogPruning) throws IOException {
        if (shutdown) {
            logShutdownMessage(triggerInfo);
            return NO_APPEND_INDEX;
        }
        var highestTransactionEver = metadataProvider.getHighestEverClosedTransaction();
        var lastClosedBatch = metadataProvider.getLastClosedBatch();
        var oldestNotVisibleTransactionInfo = evaluateOldestNotVisibleTransactionInfo(lastClosedBatch);

        return checkpointByExternalParams(
                highestTransactionEver,
                oldestNotVisibleTransactionInfo.appendIndex(),
                oldestNotVisibleTransactionInfo.logPosition(),
                lastClosedBatch.logPosition(),
                lastClosedBatch.appendIndex(),
                triggerInfo,
                skipLogPruning);
    }

    private long checkpointByExternalParams(
            TransactionId transactionId,
            long oldestNotVisibleAppendIndex,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            long appendIndex,
            TriggerInfo triggerInfo,
            boolean skipLogPruning)
            throws IOException {
        if (shutdown) {
            logShutdownMessage(triggerInfo);
            return NO_APPEND_INDEX;
        }
        return doCheckpoint(
                transactionId,
                appendIndex,
                oldestNotVisibleAppendIndex,
                oldestNotCompletedPosition,
                checkpointedLogPosition,
                triggerInfo,
                skipLogPruning);
    }

    @VisibleForTesting
    AsyncBlockAccessor createAsyncBlockAccessor(
            AsyncIOProvider asyncIOProvider, MemoryTracker memoryTracker, DatabaseFlushEvent flushEvent) {
        if (config.get(pagecache_async_io)) {
            return asyncIOProvider.createAsyncBlockAccessor(
                    128,
                    new AsyncCheckpointCompletionHandler(flushEvent),
                    new AsyncCheckpointFailureHandler(flushEvent),
                    memoryTracker);
        }
        return AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
    }

    private long doCheckpoint(
            TransactionId transactionId,
            long appendIndex,
            long oldestNotVisibleAppendIndex,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            TriggerInfo triggerInfo,
            boolean skipLogPruning)
            throws IOException {
        var databaseTracer = tracers.getDatabaseTracer();
        try (var cursorContext = cursorContextFactory.create(CHECKPOINT_TAG);
                LogCheckPointEvent checkPointEvent = databaseTracer.beginCheckPoint()) {
            long highestEverClosedTransactionId = transactionId.id();
            cursorContext.getVersionContext().initWrite(highestEverClosedTransactionId);
            KernelVersion kernelVersion = metadataProvider.kernelVersion();
            // info about last checkpoint is used only be store copy and so far we do not want to update protocol
            // to contain all the fields and state to make checkpoint identical after store copy.
            // To make it still work we are using the oldest non-visible index instead of the last batch plus state
            var ongoingCheckpoint = new LatestCheckpointInfo(
                    kernelVersion, transactionId, oldestNotVisibleAppendIndex, checkpointedLogPosition);
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

            try (var flushEvent = checkPointEvent.beginDatabaseFlush();
                    var asyncBlockAccessor =
                            createAsyncBlockAccessor(AsyncIOProvider.getInstance(), memoryTracker, flushEvent)) {
                forceOperation.flushAndForce(flushEvent, asyncBlockAccessor, cursorContext);
                flushEvent.ioControllerLimit(ioController.configuredLimit());
            }

            /*
             * Check kernel health before going to write the next check point.  In case of a panic this check point
             * will be aborted, which is the safest alternative so that the next recovery will have a chance to
             * repair the damages.
             */
            databasePanic.assertNoPanic(IOException.class);
            checkpointFile
                    .getCheckpointAppender()
                    .checkPoint(
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
            if (!skipLogPruning) {
                logPruning.pruneLogs(oldestNotCompletedPosition.getLogVersion());
            }
            latestCheckPointInfo = ongoingCheckpoint;
            return latestCheckPointInfo.appendIndex();
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
        String averageSpeedPerSecond = getAverageSpeed(checkpointEvent, durationMillis);

        String ioDetails = IO_DETAILS_TEMPLATE.formatted(
                checkpointEvent.getPagesFlushed(),
                (int) (flushRatio * 100),
                checkpointEvent.getIOsPerformed(),
                ioLimitDescription(ioLimit),
                checkpointEvent.getTimesPaused(),
                checkpointEvent.getMillisPaused(),
                averageSpeedPerSecond);
        return checkpointReason + " checkpoint completed in " + duration(durationMillis) + ". " + ioDetails;
    }

    private static String getAverageSpeed(LogCheckPointEvent checkpointEvent, long durationMillis) {
        long totalFlushedBytes = checkpointEvent.getPagesFlushed() * PageCache.PAGE_SIZE;
        long seconds = Duration.ofMillis(durationMillis).toSeconds();
        long bytesPerSecond = seconds == 0 ? totalFlushedBytes : totalFlushedBytes / seconds;
        return ByteUnit.bytesToString(bytesPerSecond) + "/s";
    }

    private String ioLimitDescription(long ioLimit) {
        if (!ioController.isEnabled() || ioLimit < 0) {
            return UNLIMITED_IO_CONTROLLER_LIMIT;
        }
        return ioController.isIopsBasedLimit()
                ? ioLimit + " IOPS"
                : ByteUnit.bytesToString(ioLimit * PageCache.PAGE_SIZE) + "/s";
    }

    private void logShutdownMessage(TriggerInfo triggerInfo) {
        log.warn("Checkpoint was requested on already shutdown checkpointer. Requester: "
                + triggerInfo.describe(UNKNOWN_CHECKPOINT_INFO));
    }

    private NotCompletedTransactionInfo evaluateOldestNotVisibleTransactionInfo(ClosedBatchMetadata lastClosedBatch) {
        var openTransactionMetadata = metadataProvider.getOldestOpenTransaction();
        if (openTransactionMetadata == null) {
            return new NotCompletedTransactionInfo(lastClosedBatch.appendIndex(), lastClosedBatch.logPosition());
        }

        long oldestBatchAppendIndex = openTransactionMetadata.appendIndex();
        // oldest not closed is after closed tx id so nothing to guard
        if (oldestBatchAppendIndex > lastClosedBatch.appendIndex()) {
            return new NotCompletedTransactionInfo(lastClosedBatch.appendIndex(), lastClosedBatch.logPosition());
        }

        return new NotCompletedTransactionInfo(
                openTransactionMetadata.appendIndex() - 1, openTransactionMetadata.logPosition());
    }

    @Override
    public LatestCheckpointInfo latestCheckPointInfo() {
        return latestCheckPointInfo;
    }

    @FunctionalInterface
    public interface ForceOperation {
        ForceOperation NO_OP = (flushEvent, asyncBlockAccessor, cursorContext) -> {};

        void flushAndForce(
                DatabaseFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext)
                throws IOException;

        default long compact(
                DatabaseFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext)
                throws IOException {
            return 0;
        }
    }

    private record NotCompletedTransactionInfo(long appendIndex, LogPosition logPosition) {}
}
