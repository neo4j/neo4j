/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com.storecopy;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionCommitProcess committed to the
 * store}, in batches.
 */
public class TransactionCommittingResponseUnpacker extends LifecycleAdapter implements ResponseUnpacker
{
    /**
     * Dependencies that this {@link TransactionCommittingResponseUnpacker} has. These are called upon
     * in {@link TransactionCommittingResponseUnpacker#start()}.
     */
    public interface Dependencies
    {
        /**
         * Responsible for committing batches of transactions received from transaction stream responses.
         */
        TransactionCommitProcess commitProcess();

        /**
         * Responsible for fulfilling transaction obligations received from transaction obligation responses.
         */
        TransactionObligationFulfiller obligationFulfiller();

        /**
         * Log provider
         */
        LogService logService();
    }

    /**
     * Common implementation which pulls out dependencies from a {@link DependencyResolver} and constructs
     * whatever components it needs from that.
     */
    private static class ResolvableDependencies implements Dependencies
    {
        private final DependencyResolver resolver;

        public ResolvableDependencies( DependencyResolver resolver )
        {
            this.resolver = resolver;
        }

        @Override
        public TransactionCommitProcess commitProcess()
        {
            // We simply can't resolve the commit process here, since the commit process of a slave
            // is one that sends transactions to the master. We here, however would like to actually
            // commit transactions in this db.
            return new TransactionRepresentationCommitProcess(
                    resolver.resolveDependency( TransactionAppender.class ),
                    resolver.resolveDependency( StorageEngine.class ) );
        }

        @Override
        public TransactionObligationFulfiller obligationFulfiller()
        {
            try
            {
                return resolver.resolveDependency( TransactionObligationFulfiller.class );
            }
            catch ( UnsatisfiedDependencyException e )
            {
                return toTxId -> {
                    throw new UnsupportedOperationException( "Should not be called" );
                };
            }
        }

        @Override
        public LogService logService()
        {
            return resolver.resolveDependency( LogService.class );
        }
    }

    public static final int DEFAULT_BATCH_SIZE = 100;

    static final String msg = "Kernel panic detected: pulled transactions cannot be applied to a non-healthy database. "
            + "In order to resolve this issue a manual restart of this instance is required.";

    // Assigned in constructor
    private final Dependencies dependencies;
    private final int maxBatchSize;

    // Assigned in start()
    private TransactionCommitProcess commitProcess;
    private TransactionObligationFulfiller obligationFulfiller;
    private Log log;

    // Assigned in stop()
    private volatile boolean stopped;

    public TransactionCommittingResponseUnpacker( DependencyResolver dependencies, int maxBatchSize )
    {
        this( new ResolvableDependencies( dependencies ), maxBatchSize );
    }

    public TransactionCommittingResponseUnpacker( Dependencies dependencies, int maxBatchSize )
    {
        this.dependencies = dependencies;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void unpackResponse( Response<?> response, TxHandler txHandler ) throws Exception
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Component is currently stopped" );
        }

        BatchingResponseHandler responseHandler = new BatchingResponseHandler( maxBatchSize,
                (batch) -> {
                    commitProcess.commit( batch, CommitEvent.NULL, EXTERNAL );
                }, obligationFulfiller, txHandler, log );
        try
        {
            response.accept( responseHandler );
        }
        finally
        {
            responseHandler.applyQueuedTransactions();
        }
    }

    @Override
    public void start()
    {
        this.commitProcess = dependencies.commitProcess();
        this.obligationFulfiller = dependencies.obligationFulfiller();
        this.log = dependencies.logService().getInternalLog( BatchingResponseHandler.class );
        this.stopped = false;
    }

    @Override
    public void stop()
    {
        this.stopped = true;
    }
}
