/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

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
public interface IndexProxy
{
    void start() throws IOException;

    IndexUpdater newUpdater( IndexUpdateMode mode );

    /**
     * Initiates dropping this index context. The returned {@link Future} can be used to await
     * its completion.
     * Must close the context as well.
     */
    Future<Void> drop() throws IOException;

    /**
     * Initiates a closing of this index context. The returned {@link Future} can be used to await
     * its completion.
     */
    Future<Void> close() throws IOException;

    IndexDescriptor getDescriptor();

    SchemaIndexProvider.Descriptor getProviderDescriptor();

    InternalIndexState getState();

    /**
     * @return failure message. Expect a call to it if {@link #getState()} returns {@link InternalIndexState#FAILED}.
     */
    IndexPopulationFailure getPopulationFailure() throws IllegalStateException;

    void force() throws IOException;

    void flush() throws IOException;

    /**
     * @throws IndexNotFoundKernelException if the index isn't online yet.
     */
    IndexReader newReader() throws IndexNotFoundKernelException;

    /**
     * @return {@code true} if the call waited, {@code false} if the condition was already reached.
     */
    boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException;

    void activate() throws IndexActivationFailedKernelException;

    void validate() throws ConstraintVerificationFailedKernelException, IndexPopulationFailedKernelException;

    ResourceIterator<File> snapshotFiles() throws IOException;

    IndexConfiguration config();
}
