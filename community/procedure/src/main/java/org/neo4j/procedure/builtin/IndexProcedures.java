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
package org.neo4j.procedure.builtin;

import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.backgroundRebuildAll;
import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.backgroundRebuildUpdated;
import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.foregroundRebuildUpdated;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
import org.neo4j.kernel.impl.api.index.IndexSamplingMode;
import org.neo4j.kernel.impl.api.index.IndexingService;

public class IndexProcedures {
    private final KernelTransaction ktx;
    private final IndexingService indexingService;

    public IndexProcedures(KernelTransaction tx, IndexingService indexingService) {
        this.ktx = tx;
        this.indexingService = indexingService;
    }

    void awaitIndexByName(String indexName, long timeout, TimeUnit timeoutUnits) throws ProcedureException {
        IndexDescriptor index = getIndex(indexName, "awaitIndexByName");
        waitUntilOnline(index, timeout, timeoutUnits, "awaitIndexByName");
    }

    void resampleIndex(String indexName) throws ProcedureException {
        IndexDescriptor index = getIndex(indexName, "resampleIndex");
        triggerSampling(index);
    }

    void resampleOutdatedIndexes() {
        indexingService.triggerIndexSampling(backgroundRebuildUpdated());
    }

    void resampleOutdatedIndexes(long timeOutSeconds) {
        long millis = TimeUnit.SECONDS.toMillis(timeOutSeconds);
        IndexSamplingMode mode = foregroundRebuildUpdated(millis);
        indexingService.triggerIndexSampling(mode);
    }

    private IndexDescriptor getIndex(String indexName, String procedureName) throws ProcedureException {
        // Find index by name.
        IndexDescriptor indexReference = ktx.schemaRead().indexGetForName(indexName);

        if (indexReference == IndexDescriptor.NO_INDEX) {
            throw ProcedureException.noSuchIndex(indexName, procedureName, true);
        }
        return indexReference;
    }

    private void waitUntilOnline(IndexDescriptor index, long timeout, TimeUnit timeoutUnits, String procedureName)
            throws ProcedureException {
        try {
            Predicates.awaitEx(() -> isOnline(index, procedureName), timeout, timeoutUnits);
        } catch (TimeoutException e) {
            throw ProcedureException.indexDidNotComeOnline(
                    procedureName, index.userDescription(ktx.tokenRead()), timeout, timeoutUnits);
        }
    }

    private boolean isOnline(IndexDescriptor index, String procedureName) throws ProcedureException {
        InternalIndexState state = getState(index, procedureName);
        return switch (state) {
            case POPULATING -> false;
            case ONLINE -> true;
            case FAILED -> {
                String cause = getFailure(index, procedureName);
                throw ProcedureException.indexInFailedState(
                        index.getName(),
                        String.format(
                                IndexPopulationFailure.appendCauseOfFailure("Index '%s' is in failed state.", cause),
                                index.getName()));
            }
        };
    }

    private InternalIndexState getState(IndexDescriptor index, String procedureName) throws ProcedureException {
        try {
            return ktx.schemaRead().indexGetState(index);
        } catch (IndexNotFoundKernelException e) {
            throw ProcedureException.noSuchIndex(index.getName(), procedureName, false);
        }
    }

    private String getFailure(IndexDescriptor index, String procedureName) throws ProcedureException {
        try {
            return ktx.schemaRead().indexGetFailure(index);
        } catch (IndexNotFoundKernelException e) {
            throw ProcedureException.noSuchIndex(index.getName(), procedureName, false);
        }
    }

    private void triggerSampling(IndexDescriptor index) {
        indexingService.triggerIndexSampling(index, backgroundRebuildAll());
    }
}
