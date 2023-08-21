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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;
import org.neo4j.values.storable.Value;

public class IndexProxyAdapter implements IndexProxy {
    @Override
    public void start() {}

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        return SwallowingIndexUpdater.INSTANCE;
    }

    @Override
    public void drop() {}

    @Override
    public InternalIndexState getState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {}

    @Override
    public void refresh() {}

    @Override
    public void close(CursorContext cursorContext) {}

    @Override
    public IndexDescriptor getDescriptor() {
        return null;
    }

    @Override
    public ValueIndexReader newValueReader() {
        return ValueIndexReader.EMPTY;
    }

    @Override
    public TokenIndexReader newTokenReader() {
        return TokenIndexReader.EMPTY;
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void activate() {}

    @Override
    public void validate() {}

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {}

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return emptyResourceIterator();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return Collections.emptyMap();
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException {
        throw new IllegalStateException("This index isn't failed");
    }

    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return PopulationProgress.NONE;
    }

    @Override
    public void reportUsageStatistics(IndexUsageStatsConsumer consumer) {}
}
