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
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.values.storable.Value;

public class FailedIndexProxy extends AbstractSwallowingIndexProxy {
    private final IndexProxyStrategy indexProxyStrategy;
    private final MinimalIndexAccessor minimalIndexAccessor;
    private final InternalLog log;

    FailedIndexProxy(
            IndexProxyStrategy indexProxyStrategy,
            MinimalIndexAccessor minimalIndexAccessor,
            IndexPopulationFailure populationFailure,
            InternalLogProvider logProvider) {
        super(indexProxyStrategy.getIndexDescriptor(), populationFailure);
        this.indexProxyStrategy = indexProxyStrategy;
        this.minimalIndexAccessor = minimalIndexAccessor;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public void start() {
        // nothing to start
    }

    @Override
    public void drop() {
        indexProxyStrategy.removeStatisticsForIndex();
        String message = "FailedIndexProxy#drop index on " + indexProxyStrategy.getIndexUserDescription()
                + " dropped due to:\n" + getPopulationFailure().asString();
        log.info(message);
        minimalIndexAccessor.drop();
    }

    @Override
    public InternalIndexState getState() {
        return InternalIndexState.FAILED;
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit) throws IndexPopulationFailedKernelException {
        throw failureCause();
    }

    private IndexPopulationFailedKernelException failureCause() {
        return getPopulationFailure()
                .asIndexPopulationFailure(getDescriptor().schema(), indexProxyStrategy.getIndexUserDescription());
    }

    @Override
    public void activate() {
        throw new UnsupportedOperationException("Cannot activate a failed index.");
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException {
        throw failureCause();
    }

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {}

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return minimalIndexAccessor.snapshotFiles();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return minimalIndexAccessor.indexConfig();
    }

    @Override
    public ValueIndexReader newValueReader() {
        throw new UnsupportedOperationException("Can not get a reader for an index in FAILED state");
    }

    @Override
    public TokenIndexReader newTokenReader() {
        throw new UnsupportedOperationException("Can not get a reader for an index in FAILED state");
    }
}
