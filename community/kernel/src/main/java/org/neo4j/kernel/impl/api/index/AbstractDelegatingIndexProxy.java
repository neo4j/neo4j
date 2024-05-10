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
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.IncompleteConstraintValidationException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;
import org.neo4j.values.storable.Value;

public abstract class AbstractDelegatingIndexProxy implements IndexProxy {
    public abstract IndexProxy getDelegate();

    @Override
    public void start() {
        getDelegate().start();
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        return getDelegate().newUpdater(mode, cursorContext, parallel);
    }

    @Override
    public void drop() {
        getDelegate().drop();
    }

    @Override
    public InternalIndexState getState() {
        return getDelegate().getState();
    }

    @Override
    public IndexDescriptor getDescriptor() {
        return getDelegate().getDescriptor();
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        getDelegate().force(flushEvent, cursorContext);
    }

    @Override
    public void refresh() throws IOException {
        getDelegate().refresh();
    }

    @Override
    public void close(CursorContext cursorContext) throws IOException {
        getDelegate().close(cursorContext);
    }

    @Override
    public ValueIndexReader newValueReader() throws IndexNotFoundKernelException {
        return getDelegate().newValueReader();
    }

    @Override
    public TokenIndexReader newTokenReader() throws IndexNotFoundKernelException {
        return getDelegate().newTokenReader();
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit)
            throws IndexPopulationFailedKernelException, InterruptedException {
        return getDelegate().awaitStoreScanCompleted(time, unit);
    }

    @Override
    public void activate() {
        getDelegate().activate();
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException, IncompleteConstraintValidationException {
        getDelegate().validate();
    }

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {
        getDelegate().validateBeforeCommit(tuple, entityId);
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException {
        return getDelegate().getPopulationFailure();
    }

    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return getDelegate().getIndexPopulationProgress();
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", getClass().getSimpleName(), getDelegate());
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return getDelegate().snapshotFiles();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return getDelegate().indexConfig();
    }

    @Override
    public void reportUsageStatistics(IndexUsageStatsConsumer consumer) {
        getDelegate().reportUsageStatistics(consumer);
    }
}
