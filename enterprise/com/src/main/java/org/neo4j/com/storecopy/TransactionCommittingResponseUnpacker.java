/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.neo4j.com.Response;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class TransactionCommittingResponseUnpacker implements ResponseUnpacker, Lifecycle
{
    private final DependencyResolver resolver;

    private TransactionAppender appender;
    private TransactionRepresentationStoreApplier storeApplier;
    private TransactionIdStore transactionIdStore;
    private TransactionObligationFulfiller obligationFulfiller;

    private volatile boolean stopped = false;

    public TransactionCommittingResponseUnpacker( DependencyResolver resolver )
    {
        this.resolver = resolver;
    }

    @Override
    public void unpackResponse( Response<?> response, final TxHandler txHandler ) throws IOException
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Component is currently stopped" );
        }

        // TODO return the future obligation future out from the unpackResponse method?
        response.accept( new Response.Handler()
        {
            @Override
            public void obligation( long txId ) throws IOException
            {
                try
                {
                    obligationFulfiller.pullUpdates( txId ).get();
                }
                catch ( InterruptedException | ExecutionException e )
                {
                    throw new IOException( e );
                }
            }

            @Override
            public Visitor<CommittedTransactionRepresentation, IOException> transactions()
            {
                // TODO This is only supposed to be run from the update puller. But this looks odd
                // the application of transactions should reside in the update puller, not here.
                // Also... the batching and stuff and what not, should go here... and there should
                // be one visitor instance and stuff.
                return new Visitor<CommittedTransactionRepresentation, IOException>()
                {
                    @Override
                    public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
                    {
                        // synchronized is needed here:
                        // read all about it at TransactionAppender#append(CommittedTransactionRepresentation)
                        synchronized ( appender )
                        {
                            long transactionId = transaction.getCommitEntry().getTxId();
                            if ( appender.append( transaction ) )
                            {
                                transactionIdStore.transactionCommitted( transactionId );
                                try
                                {
                                    try ( LockGroup locks = new LockGroup() )
                                    {
                                        storeApplier.apply( transaction.getTransactionRepresentation(), locks,
                                                transactionId, TransactionApplicationMode.EXTERNAL );
                                        txHandler.accept( transaction );
                                    }
                                }
                                finally
                                {
                                    transactionIdStore.transactionClosed( transactionId );
                                }
                            }
                        }
                        return true;
                    }
                };
            }
        } );
    }

    @Override
    public void init() throws Throwable
    {   // Nothing to init
    }

    @Override
    public void start() throws Throwable
    {
        this.appender = resolver.resolveDependency( LogicalTransactionStore.class ).getAppender();
        this.storeApplier = resolver.resolveDependency( TransactionRepresentationStoreApplier.class );
        this.transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
        this.obligationFulfiller = resolver.resolveDependency( TransactionObligationFulfiller.class );
        this.stopped = false;
    }

    @Override
    public void stop() throws Throwable
    {
        this.stopped = true;
    }

    @Override
    public void shutdown() throws Throwable
    {   // Nothing to shut down
    }
}
