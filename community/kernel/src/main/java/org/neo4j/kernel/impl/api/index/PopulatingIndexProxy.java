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

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;

public class PopulatingIndexProxy implements IndexProxy {
    private final IndexDescriptor indexDescriptor;
    private final IndexPopulationJob job;
    private final MultipleIndexPopulator.IndexPopulation indexPopulation;

    PopulatingIndexProxy(
            IndexDescriptor indexDescriptor,
            IndexPopulationJob job,
            MultipleIndexPopulator.IndexPopulation indexPopulation) {
        this.indexDescriptor = indexDescriptor;
        this.job = job;
        this.indexPopulation = indexPopulation;
    }

    @Override
    public void start() {}

    @Override
    public IndexUpdater newUpdater(final IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        return switch (mode) {
            case ONLINE, RECOVERY -> new PopulatingIndexUpdater() {
                @Override
                public void process(IndexEntryUpdate<?> update) {
                    job.update(update);
                }
            };
            default -> new PopulatingIndexUpdater() {
                @Override
                public void process(IndexEntryUpdate<?> update) {
                    throw new IllegalArgumentException("Unsupported update mode: " + mode);
                }
            };
        };
    }

    @Override
    public void drop() {
        job.dropPopulation(indexPopulation);
    }

    @Override
    public IndexDescriptor getDescriptor() {
        return indexDescriptor;
    }

    @Override
    public InternalIndexState getState() {
        return InternalIndexState.POPULATING;
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
        // Ignored... this isn't called from the outside while we're populating the index.
    }

    @Override
    public void refresh() {
        // Ignored... this isn't called from the outside while we're populating the index.
    }

    @Override
    public void close(CursorContext cursorContext) {
        job.stop(indexPopulation, cursorContext);
    }

    @Override
    public ValueIndexReader newValueReader() throws IndexNotFoundKernelException {
        throw new IndexNotFoundKernelException("Index is still populating: " + job);
    }

    @Override
    public TokenIndexReader newTokenReader() throws IndexNotFoundKernelException {
        throw new IndexNotFoundKernelException("Index is still populating: " + job);
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit) throws InterruptedException {
        return job.awaitCompletion(time, unit);
    }

    @Override
    public void activate() {
        throw new IllegalStateException("Cannot activate index while it is still populating: " + job);
    }

    @Override
    public void validate() {
        throw new IllegalStateException("Cannot validate index while it is still populating: " + job);
    }

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {
        // It's OK to put whatever values in while populating because it will take the natural path of failing the
        // population.
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return emptyResourceIterator();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return indexPopulation.populator.indexConfig();
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException {
        throw new IllegalStateException(this + " is POPULATING");
    }

    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return job.getPopulationProgress(indexPopulation);
    }

    @Override
    public void reportUsageStatistics(IndexUsageStatsConsumer consumer) {}

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[job:" + job + "]";
    }

    private abstract static class PopulatingIndexUpdater implements IndexUpdater {
        @Override
        public void close() {}
    }
}
