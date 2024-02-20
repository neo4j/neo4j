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
package org.neo4j.internal.id.indexed;

import static org.neo4j.internal.id.IdValidator.hasReservedIdInRange;
import static org.neo4j.internal.id.indexed.IdRange.ADDITION_ALL;
import static org.neo4j.internal.id.indexed.IdRange.ADDITION_REUSE;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_ALL;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_COMMIT;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_RESERVED;
import static org.neo4j.internal.id.indexed.IdRange.BITSET_REUSE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdValidator;

/**
 * Contains logic for merging ID state changes into the tree backing an {@link IndexedIdGenerator}.
 * Basically manipulates {@link IdRangeKey} and {@link IdRange} instances and sends to {@link Writer#merge(Object, Object, ValueMerger)}.
 */
class IdRangeMarker implements IdGenerator.TransactionalMarker, IdGenerator.ContextualMarker {
    private static final int TYPE_NONE = 0;
    private static final int TYPE_USED = 1;
    private static final int TYPE_DELETED = 2;
    private static final int TYPE_RESERVED = 3;
    private static final int TYPE_UNRESERVED = 4;
    private static final int TYPE_UNCACHED = 5;
    private static final int TYPE_FREE = 6;
    private static final int TYPE_DELETED_AND_FREE = 7;
    private static final int TYPE_UNALLOCATED = 8;
    private static final int TYPE_BRIDGED = 9;

    /**
     * Number of ids that is contained in one {@link IdRange}
     */
    private final int idsPerEntry;

    /**
     * {@link GBPTree} writer of id state updates.
     */
    private final Writer<IdRangeKey, IdRange> writer;

    /**
     * Lock for making this marker have exclusive access to updates. Unlocked on {@link #close()}.
     */
    private final Lock lock;

    /**
     * Which {@link GBPTree} {@link ValueMerger} to use, may be different depending on whether or not the id generator has been fully started,
     * i.e. different whether it's recovery mode or normal operations mode.
     */
    private final ValueMerger<IdRangeKey, IdRange> merger;

    /**
     * Whether or not the id generator has been started.
     */
    private final boolean started;

    /**
     * Set to true as soon as this marker marks any id as "free", so that the {@link FreeIdScanner} will go through the effort of even starting
     * a scan for free ids.
     */
    private final AtomicBoolean freeIdsNotifier;

    /**
     * Generation that this marker was instantiated at. It cannot change as long as this marker is unclosed.
     * All {@link IdRange ranges} written by this marker will get updated with this generation.
     */
    private final long generation;

    /**
     * Highest written id that this id generator has ever done. Used by this marker to fill gaps between previously highest written id
     * and the id being written with deleted ids.
     */
    private final AtomicLong highestWrittenId;

    /**
     * Whether or not to bridge gaps between previously highest written id and id being written as updates comes in.
     */
    private final boolean bridgeIdGaps;

    /**
     * When marking an ID as deleted, also immediately mark it as freed.
     */
    private final boolean deleteAlsoFrees;

    /**
     * {@link IdRangeKey} instance to populate with data as ids are being written.
     */
    private final IdRangeKey key;

    /**
     * {@link IdRange} instance to populate with data as ids are being written.
     */
    private final IdRange value;

    /**
     * Receives notifications of events.
     */
    private final IndexedIdGenerator.Monitor monitor;

    /**
     * Current type of operations. As various "mark" operations comes in they modify the {@link #key} and {@link #value}
     * states, such that if multiple operations of the same type and in the same range comes in sequence they are all written in one
     * call back to the tree before moving over to the other range/operation.
     */
    private int type = TYPE_NONE;

    IdRangeMarker(
            int idsPerEntry,
            Layout<IdRangeKey, IdRange> layout,
            Writer<IdRangeKey, IdRange> writer,
            Lock lock,
            ValueMerger<IdRangeKey, IdRange> merger,
            boolean started,
            AtomicBoolean freeIdsNotifier,
            long generation,
            AtomicLong highestWrittenId,
            boolean bridgeIdGaps,
            boolean deleteAlsoFrees,
            IndexedIdGenerator.Monitor monitor) {
        this.idsPerEntry = idsPerEntry;
        this.writer = writer;
        this.key = layout.newKey();
        this.value = layout.newValue();
        this.lock = lock;
        this.merger = merger;
        this.started = started;
        this.freeIdsNotifier = freeIdsNotifier;
        this.generation = generation;
        this.highestWrittenId = highestWrittenId;
        this.bridgeIdGaps = bridgeIdGaps;
        this.deleteAlsoFrees = deleteAlsoFrees;
        this.monitor = monitor;
    }

    @Override
    public void close() {
        try {
            try {
                flushRange();
            } finally {
                writer.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (lock != null) {
                lock.unlock();
            }
            monitor.markSessionDone();
        }
    }

    @Override
    public void markUsed(long id, int numberOfIds) {
        bridgeGapBetweenHighestWrittenIdAndThisId(id, numberOfIds, false);
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            prepareRange(TYPE_USED, id, false);
            value.setBits(BITSET_ALL, idOffset(id), numberOfIds);
            monitor.markedAsUsed(id, numberOfIds);
        }
    }

    @Override
    public void markDeleted(long id, int numberOfIds) {
        if (!deleteAlsoFrees) {
            if (!hasReservedIdInRange(id, id + numberOfIds)) {
                prepareRange(TYPE_DELETED, id, true);
                value.setBits(BITSET_COMMIT, idOffset(id), numberOfIds);
                monitor.markedAsDeleted(id, numberOfIds);
            }
        } else {
            markDeletedAndFree(id, numberOfIds);
        }
    }

    @Override
    public void markReserved(long id, int numberOfIds) {
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            prepareRange(TYPE_RESERVED, id, true);
            value.setBits(BITSET_RESERVED, idOffset(id), numberOfIds);
            monitor.markedAsReserved(id, numberOfIds);
        }
    }

    @Override
    public void markUnreserved(long id, int numberOfIds) {
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            prepareRange(TYPE_UNRESERVED, id, false);
            value.setBits(BITSET_RESERVED, idOffset(id), numberOfIds);
            monitor.markedAsUnreserved(id, numberOfIds);
        }
    }

    @Override
    public void markUncached(long id, int numberOfIds) {
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            // Mark free:1, reserved:0
            prepareRange(TYPE_UNCACHED, id, ADDITION_REUSE);
            var idOffset = idOffset(id);
            value.setBits(BITSET_REUSE, idOffset, numberOfIds);
            value.setBits(BITSET_RESERVED, idOffset, numberOfIds);
            monitor.markedAsFree(id, numberOfIds);
            monitor.markedAsUnreserved(id, numberOfIds);
        }
    }

    @Override
    public void markFree(long id, int numberOfIds) {
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            prepareRange(TYPE_FREE, id, true);
            value.setBits(BITSET_REUSE, idOffset(id), numberOfIds);
            monitor.markedAsFree(id, numberOfIds);
        }
    }

    @Override
    public void markDeletedAndFree(long id, int numberOfIds) {
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            prepareRange(TYPE_DELETED_AND_FREE, id, true);
            var idOffset = idOffset(id);
            value.setBits(BITSET_COMMIT, idOffset, numberOfIds);
            value.setBits(BITSET_REUSE, idOffset, numberOfIds);
            monitor.markedAsDeleted(id, numberOfIds);
            monitor.markedAsFree(id, numberOfIds);
        }
    }

    @Override
    public void markUnallocated(long id, int numberOfIds) {
        bridgeGapBetweenHighestWrittenIdAndThisId(id, numberOfIds, true);
        if (!hasReservedIdInRange(id, id + numberOfIds)) {
            markWithSupportForLargerThanRange(
                    TYPE_UNALLOCATED, id, numberOfIds, ADDITION_REUSE, BITSET_REUSE, BITSET_RESERVED);
            monitor.markedAsFree(id, numberOfIds);
            monitor.markedAsUnreserved(id, numberOfIds);
        }
    }

    private void markWithSupportForLargerThanRange(
            int type, long id, long numberOfIds, byte addition, int firstBitSet, int secondBitSet) {
        var idOffset = idOffset(id);
        while (numberOfIds > 0) {
            prepareRange(type, id, addition);
            int numberOfIdsInThisRange = (int) Math.min(numberOfIds, idsPerEntry - idOffset);
            value.setBits(firstBitSet, idOffset, numberOfIdsInThisRange);
            if (secondBitSet != -1) {
                value.setBits(secondBitSet, idOffset, numberOfIdsInThisRange);
            }
            idOffset = 0;
            id += numberOfIdsInThisRange;
            numberOfIds -= numberOfIdsInThisRange;
        }
    }

    private void prepareRange(int newType, long id, boolean addition) {
        prepareRange(newType, id, addition ? ADDITION_ALL : 0);
    }

    private void prepareRange(int newType, long id, byte addition) {
        long idRangeIdx = idRangeIndex(id);
        if (newType != type || idRangeIdx != key.getIdRangeIdx()) {
            flushRange();
            type = newType;
            key.setIdRangeIdx(idRangeIdx);
            value.clear(generation, addition);
        }
    }

    /**
     * Flushes changes in the current range, i.e. writes them to the tree based on the currently set type of marks.
     * After write the current type is set to {@link #TYPE_NONE}.
     */
    private void flushRange() {
        if (type == TYPE_USED) {
            writer.mergeIfExists(key, value, merger);
        } else if (type != TYPE_NONE) {
            writer.merge(key, value, merger);
            if (type == TYPE_FREE || type == TYPE_DELETED_AND_FREE || type == TYPE_UNALLOCATED) {
                freeIdsNotifier.set(true);
            }
        }
        type = TYPE_NONE;
    }

    private long idRangeIndex(long id) {
        return id / idsPerEntry;
    }

    private int idOffset(long id) {
        return (int) (id % idsPerEntry);
    }

    /**
     * Fills the space between the previously highest ever written id and the id currently being updated. The ids between those two points
     * will be marked as deleted, or in the recovery case (where {@link #started} is {@code false} marked as deleted AND free.
     * This solves a problem of not losing track of ids that have been allocated off of high id, but either not committed or failed to be committed.
     * @param id the id being updated.
     * @param numberOfIds number of ids this id allocation is.
     */
    private void bridgeGapBetweenHighestWrittenIdAndThisId(long id, int numberOfIds, boolean includeThis) {
        long highestWrittenId = this.highestWrittenId.get();
        long to = includeThis ? id + numberOfIds : id;
        if (bridgeIdGaps && highestWrittenId < to) {
            if (highestWrittenId < to - 1) {
                long bridgeId = highestWrittenId + 1;
                long bridgeNumberOfIds = to - bridgeId;
                if (IdValidator.hasReservedIdInRange(bridgeId, bridgeId + bridgeNumberOfIds)) {
                    // If we happen to bridge across the reserved ID then divide it up in two
                    // chunks: one before the reserved ID and the rest after.
                    long idsBefore = bridgeId - IdValidator.INTEGER_MINUS_ONE;
                    if (idsBefore > 0) {
                        markWithSupportForLargerThanRange(
                                TYPE_BRIDGED,
                                bridgeId,
                                idsBefore,
                                ADDITION_ALL,
                                BITSET_COMMIT,
                                started ? -1 : BITSET_REUSE);
                        monitor.bridged(bridgeId, idsBefore);
                    }
                    bridgeId += (idsBefore + 1);
                    bridgeNumberOfIds -= (idsBefore + 1);
                }

                markWithSupportForLargerThanRange(
                        TYPE_BRIDGED,
                        bridgeId,
                        bridgeNumberOfIds,
                        ADDITION_ALL,
                        BITSET_COMMIT,
                        started ? -1 : BITSET_REUSE);
                monitor.bridged(bridgeId, bridgeNumberOfIds);
            }

            // Well, we bridged the gap up and including id - 1, but we know that right after this the actual id
            // will be written so to try to isolate updates to highestWrittenId to this method we can might as well
            // do that right here.
            this.highestWrittenId.set(id + numberOfIds - 1);
        }
    }
}
