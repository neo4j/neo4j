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
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.IncompleteConstraintValidationException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.stats.IndexUsageStatsConsumer;
import org.neo4j.values.storable.Value;

/**
 * Controls access to {@link IndexPopulator}, {@link IndexAccessor} during different stages
 * of the lifecycle of an index. It's designed to be decorated with multiple stacked instances.
 *
 * The contract of {@link IndexProxy} is
 *
 * <ul>
 *     <li>The index may not be created twice</li>
 *     <li>The context may not be closed twice</li>
 *     <li>Close or drop both close the context</li>
 *     <li>The index may not be dropped before it has been created</li>
 *     <li>newUpdater and force may only be called after the index has been created and before it is closed</li>
 *     <li>It is an error to not close an updater before doing any other call on an index</li>
 *     <li>It is an error to close or drop the index while there are still ongoing calls to update and force</li>
 * </ul>
 *
 * @see ContractCheckingIndexProxy
 */
public interface IndexProxy extends MinimalIndexAccessor {
    void start();

    IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel);

    /**
     * Close this index context.
     */
    void close(CursorContext cursorContext) throws IOException;

    IndexDescriptor getDescriptor();

    InternalIndexState getState();

    /**
     * @return failure message. Expect a call to it if {@link #getState()} returns {@link InternalIndexState#FAILED}.
     */
    IndexPopulationFailure getPopulationFailure() throws IllegalStateException;

    PopulationProgress getIndexPopulationProgress();

    void force(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException;

    void refresh() throws IOException;

    /**
     * @throws IndexNotFoundKernelException if the index isn't online yet.
     * @throws UnsupportedOperationException if underlying index is not Value Index
     */
    ValueIndexReader newValueReader() throws IndexNotFoundKernelException;

    /**
     * @throws IndexNotFoundKernelException if the index isn't online yet.
     * @throws UnsupportedOperationException if underlying index is not Token Index
     */
    TokenIndexReader newTokenReader() throws IndexNotFoundKernelException;

    /**
     * @param time time to wait maximum. A value of 0 means indefinite wait.
     * @param unit unit of time to wait.
     * @return {@code true} if the call waited, {@code false} if the condition was already reached.
     */
    boolean awaitStoreScanCompleted(long time, TimeUnit unit)
            throws IndexPopulationFailedKernelException, InterruptedException;

    void activate() throws IndexActivationFailedKernelException;

    /**
     * @throws IncompleteConstraintValidationException if the data is violating the constraint.
     * This Exception should be caught and turned into a complete exception by the caller of this
     * method.
     */
    void validate() throws IndexPopulationFailedKernelException, IncompleteConstraintValidationException;

    /**
     * Validates a {@link Value} so that it's OK to later apply to the index. This method is designed to be
     * called before committing a transaction as to prevent exception during applying that transaction.
     *
     * @param tuple {@link Value value tuple} to validate.
     * @param entityId id of the entity being validated
     */
    void validateBeforeCommit(Value[] tuple, long entityId);

    /**
     * Reports usage statistics since last reported, to the {@code consumer}.
     */
    void reportUsageStatistics(IndexUsageStatsConsumer consumer);
}
