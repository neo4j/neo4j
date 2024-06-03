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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

public class OnlineIndexProxy implements IndexProxy {
    private final IndexProxyStrategy indexProxyStrategy;
    final IndexAccessor accessor;
    private final IndexUsageTracking usageTracking;
    private final DatabaseIndexStats indexCounters;
    private boolean started;

    // About this flag: there are two online "modes", you might say...
    // - One is the pure starting of an already online index which was cleanly shut down and all that.
    //   This scenario is simple and doesn't need this idempotency mode.
    // - The other is the creation or starting from an uncompleted population, where there will be a point
    //   in the future where this index will flip from a populating index proxy to an online index proxy.
    //   This is the problematic part. You see... we have been accidentally relying on the short-lived node
    //   entity locks for this to work. The scenario where they have saved indexes from getting duplicate
    //   nodes in them (one from populator and the other from a "normal" update is where a populator is nearing
    //   its completion and wants to flip. Another thread is in the middle of applying a transaction which
    //   in the end will feed an update to this index. Index updates are applied after store updates, so
    //   the populator may see the created node and add it, index flips and then the updates comes in to the normal
    //   online index and gets added again. The read lock here will have the populator wait for the transaction
    //   to fully apply, e.g. also wait for the index update to reach the population job before adding that node
    //   and flipping (the update mechanism in a populator is idempotent).
    //     This strategy has changed slightly in 3.0 where transactions can be applied in whole batches
    //   and index updates for the whole batch will be applied in the end. This is fine for everything except
    //   the above scenario because the short-lived entity locks are per transaction, not per batch, and must
    //   be so to not interfere with transactions creating constraints inside this batch. We do need to apply
    //   index updates in batches because nowadays slave update pulling and application isn't special in any
    //   way, it's simply applying transactions in batches and this needs to be very fast to not have instances
    //   fall behind in a cluster.
    //     So the sum of this is that during the session (until the next restart of the db) an index gets created
    //   it will be in this forced idempotency mode where it applies additions idempotently, which may be
    //   slightly more costly, but shouldn't make that big of a difference hopefully.
    private final boolean forcedIdempotentMode;

    OnlineIndexProxy(
            IndexProxyStrategy indexProxyStrategy,
            IndexAccessor accessor,
            boolean forcedIdempotentMode,
            IndexUsageTracking usageTracking,
            DatabaseIndexStats indexCounters) {
        this.usageTracking = usageTracking;
        assert accessor != null;
        this.indexProxyStrategy = indexProxyStrategy;
        this.accessor = accessor;
        this.forcedIdempotentMode = forcedIdempotentMode;
        this.indexCounters = indexCounters;
    }

    @Override
    public void start() {
        started = true;
    }

    @Override
    public IndexUpdater newUpdater(final IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        IndexUpdater actual = accessor.newUpdater(escalateModeIfNecessary(mode), cursorContext, parallel);
        return started ? updateCountingUpdater(actual) : actual;
    }

    private IndexUpdateMode escalateModeIfNecessary(IndexUpdateMode mode) {
        if (forcedIdempotentMode) {
            // If this proxy is flagged with taking extra care about idempotency then escalate ONLINE to
            // ONLINE_IDEMPOTENT.
            if (mode != IndexUpdateMode.ONLINE) {
                throw new IllegalArgumentException("Unexpected mode " + mode + " given that " + this
                        + " has been marked with forced idempotent mode. Expected mode " + IndexUpdateMode.ONLINE);
            }
            return IndexUpdateMode.ONLINE_IDEMPOTENT;
        }
        return mode;
    }

    private IndexUpdater updateCountingUpdater(final IndexUpdater indexUpdater) {
        return new UpdateCountingIndexUpdater(indexProxyStrategy, indexUpdater);
    }

    @Override
    public void drop() {
        indexProxyStrategy.removeStatisticsForIndex();
        accessor.drop();
    }

    @Override
    public IndexDescriptor getDescriptor() {
        return indexProxyStrategy.getIndexDescriptor();
    }

    @Override
    public InternalIndexState getState() {
        return InternalIndexState.ONLINE;
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
        accessor.force(flushEvent, cursorContext);
    }

    @Override
    public void refresh() {
        accessor.refresh();
    }

    @Override
    public void close(CursorContext cursorContext) throws IOException {
        accessor.close();
    }

    @Override
    public ValueIndexReader newValueReader() {
        return accessor.newValueReader(usageTracking.track());
    }

    @Override
    public TokenIndexReader newTokenReader() {
        return accessor.newTokenReader(usageTracking.track());
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit) {
        return false; // the store scan is already completed
    }

    @Override
    public void activate() {
        // ok, already active
    }

    @Override
    public void validate() {
        // ok, it's online so it's valid
    }

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {
        accessor.validateBeforeCommit(entityId, tuple);
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException {
        throw new IllegalStateException(this + " is ONLINE");
    }

    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return PopulationProgress.DONE;
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return accessor.snapshotFiles();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return accessor.indexConfig();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[accessor:" + accessor + ", descriptor:"
                + indexProxyStrategy.getIndexDescriptor() + "]";
    }

    @VisibleForTesting
    public IndexAccessor accessor() {
        return accessor;
    }

    @Override
    public void reportUsageStatistics(IndexUsageStatsConsumer consumer) {
        final var descriptor = getDescriptor();
        final var stats = usageTracking.getAndReset();
        indexCounters.reportQueryCount(descriptor, stats.readCount());
        consumer.addUsageStats(descriptor.getId(), stats);
    }

    @Override
    public void maintenance() {
        accessor.maintenance();
    }
}
