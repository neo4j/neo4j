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
package org.neo4j.internal.counts;

import static java.lang.Long.max;
import static org.neo4j.internal.batchimport.RecordIdIterator.forwards;
import static org.neo4j.internal.batchimport.RecordIdIterator.withProgress;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.NO_MONITOR;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LoggerPrintWriterAdaptor;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryTracker;

/**
 * Scans the store and rebuilds the {@link GBPTreeRelationshipGroupDegreesStore} contents if the file is missing.
 */
public class DegreesRebuildFromStore implements DegreesRebuilder {
    private final PageCache pageCache;
    private final NeoStores neoStores;
    private final DatabaseLayout databaseLayout;
    private final CursorContextFactory contextFactory;
    private final InternalLog log;
    private final Configuration processingConfig;

    public DegreesRebuildFromStore(
            PageCache pageCache,
            NeoStores neoStores,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            InternalLogProvider logProvider,
            Configuration processingConfig) {
        this.pageCache = pageCache;
        this.neoStores = neoStores;
        this.databaseLayout = databaseLayout;
        this.contextFactory = contextFactory;
        this.log = logProvider.getLog(DegreesRebuildFromStore.class);
        this.processingConfig = processingConfig;
    }

    @Override
    public long lastCommittedTxId() {
        return neoStores.getMetaDataStore().getLastCommittedTransactionId();
    }

    @Override
    public void rebuild(DegreeUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (neoStores.getRelationshipGroupStore().isEmpty()) {
            return;
        }

        log.warn("Missing relationship degrees store, rebuilding it.");
        NumberArrayFactory numberArrayFactory = NumberArrayFactories.auto(
                pageCache,
                contextFactory,
                databaseLayout.databaseDirectory(),
                true,
                NO_MONITOR,
                NullLog.getInstance(),
                databaseLayout.getDatabaseName());
        var loggerPrintWriterAdaptor = new LoggerPrintWriterAdaptor(log, Level.INFO);
        var totalCount = neoStores.getRelationshipGroupStore().getIdGenerator().getHighId()
                + neoStores.getRelationshipStore().getIdGenerator().getHighId();
        try (GroupDegreesCache cache = new GroupDegreesCache(
                        numberArrayFactory,
                        neoStores.getNodeStore().getIdGenerator().getHighId(),
                        memoryTracker);
                var progressListener =
                        ProgressMonitorFactory.textual(loggerPrintWriterAdaptor).singlePart("rebuild", totalCount); ) {
            superviseDynamicExecution(new PrepareCacheStage(
                    processingConfig, neoStores.getRelationshipGroupStore(), cache, contextFactory, progressListener));
            if (cache.hasAnyGroup()) {
                superviseDynamicExecution(new CalculateDegreesStage(
                        processingConfig, neoStores.getRelationshipStore(), cache, contextFactory, progressListener));
            }
            cache.writeTo(updater);
        }
        log.warn("Relationship degrees store rebuild completed.");
    }

    private static class GroupDegreesCache implements AutoCloseable {
        private static final int SHIFT_DIRECTION_BITS = 32;
        private static final int NUM_GROUP_DATA_FIELDS = 3;
        private static final int DIRECTION_OUTGOING = 0;
        private static final int DIRECTION_INCOMING = 1;
        private static final int DIRECTION_LOOP = 2;

        private final LongArray nodeCache;
        private final LongArray groupCache;
        private final AtomicLong nextGroupLocation = new AtomicLong();
        private final long highNodeId;

        GroupDegreesCache(NumberArrayFactory numberArrayFactory, long highNodeId, MemoryTracker memoryTracker) {
            this.highNodeId = highNodeId;
            this.nodeCache = numberArrayFactory.newLongArray(highNodeId, -1, memoryTracker);
            this.groupCache = numberArrayFactory.newDynamicLongArray(max(1_000_000, highNodeId / 10), 0, memoryTracker);
        }

        @Override
        public void close() {
            IOUtils.closeAllUnchecked(nodeCache, groupCache);
        }

        void addGroup(RelationshipGroupRecord groupRecord, StripedLatches latches) {
            // One long for the type + "has external degrees" bits
            // One long for the next pointer
            // One long for the groupId
            // N longs for the count slots
            int slotsForCounts = slotsForDegrees(groupRecord);
            long groupIndex = nextGroupLocation.getAndAdd(NUM_GROUP_DATA_FIELDS + slotsForCounts);
            groupCache.set(groupIndex, buildGroup(groupRecord));
            groupCache.set(groupIndex + 2, groupRecord.getId());

            long owningNode = groupRecord.getOwningNode();
            long existingGroupIndex;
            try (LatchResource latch = latches.acquire(owningNode)) {
                existingGroupIndex = nodeCache.get(owningNode);
                nodeCache.set(owningNode, groupIndex);
            }
            groupCache.set(groupIndex + 1, existingGroupIndex);
        }

        private long buildGroup(RelationshipGroupRecord groupRecord) {
            long group = groupRecord.getType();
            group |= groupRecord.hasExternalDegreesOut() ? directionBitMask(DIRECTION_OUTGOING) : 0;
            group |= groupRecord.hasExternalDegreesIn() ? directionBitMask(DIRECTION_INCOMING) : 0;
            group |= groupRecord.hasExternalDegreesLoop() ? directionBitMask(DIRECTION_LOOP) : 0;
            return group;
        }

        private int slotsForDegrees(RelationshipGroupRecord groupRecord) {
            int slots = 0;
            slots += groupRecord.hasExternalDegreesOut() ? 1 : 0;
            slots += groupRecord.hasExternalDegreesIn() ? 1 : 0;
            slots += groupRecord.hasExternalDegreesLoop() ? 1 : 0;
            return slots;
        }

        private void include(long node, int type, int directionBit, StripedLatches latches) {
            long groupIndex = nodeCache.get(node);
            while (groupIndex != -1) {
                long group = groupCache.get(groupIndex);
                if (typeOf(group) == type) {
                    long directionSlot = slotForDirection(group, directionBit);
                    if (directionSlot == -1) {
                        // Type/direction combination not tracked, skip it
                        return;
                    }

                    // Time to include it in the count
                    try (LatchResource latch = latches.acquire(groupIndex)) {
                        long prevCount = groupCache.get(groupIndex + directionSlot);
                        groupCache.set(groupIndex + directionSlot, prevCount + 1);
                        break;
                    }
                }
                groupIndex = groupCache.get(groupIndex + 1);
            }
        }

        private int slotForDirection(long group, int directionBit) {
            if (!hasDirectionBit(group, directionBit)) {
                return -1;
            }
            int slot = 0;
            for (int i = 0; i < directionBit; i++) {
                if (hasDirectionBit(group, i)) {
                    slot++;
                }
            }
            return NUM_GROUP_DATA_FIELDS + slot;
        }

        private boolean hasDirectionBit(long group, int directionBit) {
            return (group & directionBitMask(directionBit)) != 0;
        }

        private long directionBitMask(int directionBit) {
            return 1L << (SHIFT_DIRECTION_BITS + directionBit);
        }

        private int typeOf(long group) {
            return (int) group;
        }

        void writeTo(DegreeUpdater updater) {
            for (long node = 0; node < highNodeId; node++) {
                long groupIndex = nodeCache.get(node);
                while (groupIndex != -1) {
                    long group = groupCache.get(groupIndex);
                    long groupId = groupCache.get(groupIndex + 2);
                    long slot = groupIndex + NUM_GROUP_DATA_FIELDS;
                    if (hasDirectionBit(group, DIRECTION_OUTGOING)) {
                        updater.increment(groupId, OUTGOING, groupCache.get(slot));
                        slot++;
                    }
                    if (hasDirectionBit(group, DIRECTION_INCOMING)) {
                        updater.increment(groupId, INCOMING, groupCache.get(slot));
                        slot++;
                    }
                    if (hasDirectionBit(group, DIRECTION_LOOP)) {
                        updater.increment(groupId, LOOP, groupCache.get(slot));
                    }
                    groupIndex = groupCache.get(groupIndex + 1);
                }
            }
        }

        boolean hasAnyGroup() {
            return nextGroupLocation.get() > 0;
        }
    }

    private static class PrepareCacheStage extends Stage {
        PrepareCacheStage(
                Configuration config,
                RelationshipGroupStore store,
                GroupDegreesCache cache,
                CursorContextFactory cursorContextFactory,
                ProgressListener progress) {
            super("Prepare cache", null, config, Step.RECYCLE_BATCHES);
            add(new BatchFeedStep(
                    control(),
                    config,
                    withProgress(
                            forwards(
                                    store.getNumberOfReservedLowIds(),
                                    store.getIdGenerator().getHighId(),
                                    config),
                            progress),
                    store.getRecordSize()));
            add(new ReadRecordsStep<>(control(), config, false, store, cursorContextFactory));
            add(new PrepareCacheStep(control(), config, cache, cursorContextFactory));
        }
    }

    private static class PrepareCacheStep extends ProcessorStep<RelationshipGroupRecord[]> {
        private final StripedLatches latches = new StripedLatches();
        private final GroupDegreesCache cache;

        PrepareCacheStep(
                StageControl control,
                Configuration config,
                GroupDegreesCache cache,
                CursorContextFactory cursorContextFactory) {
            super(control, "PREPARE", config, config.maxNumberOfWorkerThreads(), cursorContextFactory);
            this.cache = cache;
        }

        @Override
        protected void process(RelationshipGroupRecord[] batch, BatchSender sender, CursorContext cursorContext)
                throws Throwable {
            for (RelationshipGroupRecord record : batch) {
                if (record.inUse()
                        && (record.hasExternalDegreesOut()
                                || record.hasExternalDegreesIn()
                                || record.hasExternalDegreesLoop())) {
                    cache.addGroup(record, latches);
                }
            }
        }
    }

    private static class CalculateDegreesStage extends Stage {
        CalculateDegreesStage(
                Configuration config,
                RelationshipStore store,
                GroupDegreesCache cache,
                CursorContextFactory cursorContextFactory,
                ProgressListener progress) {
            super("Calculate degrees", null, config, Step.RECYCLE_BATCHES);
            add(new BatchFeedStep(
                    control(),
                    config,
                    withProgress(forwards(0, store.getIdGenerator().getHighId(), config), progress),
                    store.getRecordSize()));
            add(new ReadRecordsStep<>(control(), config, false, store, cursorContextFactory));
            add(new CalculateDegreesStep(control(), config, cache, cursorContextFactory));
        }
    }

    private static class CalculateDegreesStep extends ProcessorStep<RelationshipRecord[]> {
        private final StripedLatches latches = new StripedLatches();
        private final GroupDegreesCache cache;

        CalculateDegreesStep(
                StageControl control,
                Configuration config,
                GroupDegreesCache cache,
                CursorContextFactory cursorContextFactory) {
            super(control, "CALCULATE", config, config.maxNumberOfWorkerThreads(), cursorContextFactory);
            this.cache = cache;
        }

        @Override
        protected void process(RelationshipRecord[] batch, BatchSender sender, CursorContext cursorContext)
                throws Throwable {
            for (RelationshipRecord record : batch) {
                if (record.inUse()) {
                    if (record.getFirstNode() == record.getSecondNode()) {
                        process(record.getFirstNode(), record.getType(), GroupDegreesCache.DIRECTION_LOOP);
                    } else {
                        process(record.getFirstNode(), record.getType(), GroupDegreesCache.DIRECTION_OUTGOING);
                        process(record.getSecondNode(), record.getType(), GroupDegreesCache.DIRECTION_INCOMING);
                    }
                }
            }
        }

        private void process(long node, int type, int directionBit) {
            cache.include(node, type, directionBit, latches);
        }
    }

    private static class StripedLatches {
        private static final int NUM_LATCHES = 1024;
        private static final int LATCH_STRIPE_MASK = Integer.highestOneBit(NUM_LATCHES) - 1;

        private final LatchResource[] latches = new LatchResource[NUM_LATCHES];

        StripedLatches() {
            for (int i = 0; i < latches.length; i++) {
                latches[i] = new LatchResource();
            }
        }

        LatchResource acquire(long id) {
            int index = (int) (id & LATCH_STRIPE_MASK);
            return latches[index].acquire();
        }
    }

    private static class LatchResource implements AutoCloseable {
        private final Lock lock = new ReentrantLock();

        LatchResource acquire() {
            lock.lock();
            return this;
        }

        @Override
        public void close() {
            lock.unlock();
        }
    }
}
