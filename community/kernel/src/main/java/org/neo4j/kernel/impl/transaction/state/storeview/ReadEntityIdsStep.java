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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Interrupted;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.ProcessContext;
import org.neo4j.internal.batchimport.staging.PullingProducerStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ReadEntityIdsStep extends PullingProducerStep<ReadEntityIdsStep.ReadEntityProcessContext> {
    private static final String CURSOR_TRACER_TAG = "indexPopulationReadEntityIds";

    private final StoreScan.ExternalUpdatesCheck externalUpdatesCheck;
    private final AtomicBoolean continueScanning;
    private final boolean canDetermineExternalUpdatesCutOffPoint;
    private final BiFunction<CursorContext, StoreCursors, EntityIdIterator> entityIdIteratorSupplier;
    private final Function<CursorContext, StoreCursors> storeCursorsFactory;
    private final CursorContextFactory contextFactory;
    private final AtomicLong position = new AtomicLong();
    private volatile long lastEntityId;

    public ReadEntityIdsStep(
            StageControl control,
            Configuration configuration,
            BiFunction<CursorContext, StoreCursors, EntityIdIterator> entityIdIteratorSupplier,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            CursorContextFactory contextFactory,
            StoreScan.ExternalUpdatesCheck externalUpdatesCheck,
            AtomicBoolean continueScanning,
            boolean canDetermineExternalUpdatesCutOffPoint) {
        super(control, configuration);
        this.entityIdIteratorSupplier = entityIdIteratorSupplier;
        this.storeCursorsFactory = storeCursorsFactory;
        this.contextFactory = contextFactory;
        this.externalUpdatesCheck = externalUpdatesCheck;
        this.continueScanning = continueScanning;
        this.canDetermineExternalUpdatesCutOffPoint = canDetermineExternalUpdatesCutOffPoint;
    }

    @Override
    protected ReadEntityProcessContext processContext() {
        return new ReadEntityProcessContext(contextFactory, storeCursorsFactory, entityIdIteratorSupplier);
    }

    @Override
    protected Object nextBatchOrNull(long ticket, int batchSize, ReadEntityProcessContext processContext) {
        if (!continueScanning.get() || !processContext.entityIdIterator.hasNext()) {
            return null;
        }

        checkAndApplyExternalUpdates(processContext.entityIdIterator);

        long[] entityIds = new long[batchSize];
        int cursor = 0;
        while (cursor < batchSize && processContext.entityIdIterator.hasNext()) {
            entityIds[cursor++] = processContext.entityIdIterator.next();
        }
        position.getAndAdd(cursor);
        lastEntityId = entityIds[cursor - 1];
        return cursor == entityIds.length ? entityIds : Arrays.copyOf(entityIds, cursor);
    }

    private void checkAndApplyExternalUpdates(EntityIdIterator entityIdIterator) {
        if (externalUpdatesCheck.needToApplyExternalUpdates()) {
            // Block here until all batches that have been sent already have been fully processed by the downstream
            // steps
            // control.isIdle returns true when all steps in this processing stage have processed all batches they have
            // received
            for (long i = 0; !control.isIdle(); i++) {
                incrementalBackoff(i);
            }
            // The scenario where the external updates cut-off point cannot be determined is that the scan isn't
            // strictly sequential,
            // for example for node-based relationship scan where the scan progresses through nodes sequentially, but
            // for each node
            // visits its relationship and it's the relationships that are "seen" by the relationship scan. The only
            // effect this has is
            // that all external updates will be applied to the population, instead of only the external updates
            // "behind" the current scan point.
            externalUpdatesCheck.applyExternalUpdates(
                    canDetermineExternalUpdatesCutOffPoint ? lastEntityId : Long.MAX_VALUE);
            entityIdIterator.invalidateCache();
        }
    }

    private static void incrementalBackoff(long iteration) {
        if (iteration < 1000) {
            Thread.onSpinWait();
            return;
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new LockAcquisitionTimeoutException(Interrupted, "Interrupted while waiting.");
        }
    }

    @Override
    protected long position() {
        return position.get();
    }

    static class ReadEntityProcessContext implements ProcessContext {
        private final CursorContext cursorContext;
        private final StoreCursors storeCursors;
        private final EntityIdIterator entityIdIterator;

        ReadEntityProcessContext(
                CursorContextFactory contextFactory,
                Function<CursorContext, StoreCursors> storeCursorsFactory,
                BiFunction<CursorContext, StoreCursors, EntityIdIterator> entityIdIteratorSupplier) {
            cursorContext = contextFactory.create(CURSOR_TRACER_TAG);
            storeCursors = storeCursorsFactory.apply(cursorContext);
            entityIdIterator = entityIdIteratorSupplier.apply(cursorContext, storeCursors);
        }

        @Override
        public void close() {
            IOUtils.closeAllUnchecked(entityIdIterator, storeCursors, cursorContext);
        }
    }
}
