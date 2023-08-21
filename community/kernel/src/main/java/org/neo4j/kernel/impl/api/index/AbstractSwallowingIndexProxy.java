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

import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;

public abstract class AbstractSwallowingIndexProxy implements IndexProxy {
    private final IndexDescriptor indexDescriptor;
    private final IndexPopulationFailure populationFailure;

    AbstractSwallowingIndexProxy(IndexDescriptor indexDescriptor, IndexPopulationFailure populationFailure) {
        this.indexDescriptor = indexDescriptor;
        this.populationFailure = populationFailure;
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() {
        return populationFailure;
    }

    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return PopulationProgress.NONE;
    }

    @Override
    public void start() {
        String message = "Unable to start index, it is in a " + getState().name() + " state.";
        throw new UnsupportedOperationException(message + ", caused by: " + getPopulationFailure());
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        return SwallowingIndexUpdater.INSTANCE;
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {}

    @Override
    public void refresh() {}

    @Override
    public IndexDescriptor getDescriptor() {
        return indexDescriptor;
    }

    @Override
    public void close(CursorContext cursorContext) {}

    @Override
    public ValueIndexReader newValueReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TokenIndexReader newTokenReader() throws IndexNotFoundKernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reportUsageStatistics(IndexUsageStatsConsumer consumer) {}
}
