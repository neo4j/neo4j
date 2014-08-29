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

import org.neo4j.com.Response;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class TransactionCommittingResponseUnpacker extends ResponseUnpacker.Adapter implements Lifecycle
{
    private final DependencyResolver resolver;

    private TransactionAppender appender;
    private TransactionRepresentationStoreApplier storeApplier;
    private TransactionIdStore transactionIdStore;

    public TransactionCommittingResponseUnpacker( DependencyResolver resolver)
    {
        this.resolver = resolver;
    }

    @Override
    public <T> T unpackResponse( Response<T> response, final TxHandler handler ) throws IOException
    {
        response.accept( new Visitor<CommittedTransactionRepresentation, IOException>()
        {
            @Override
            public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
            {
                // synchronized is needed here:
                // read all about it at TransactionAppender#append(CommittedTransactionRepresentation)
                synchronized ( appender )
                {
                    if ( appender.append( transaction ) )
                    {
                        final long transactionId = transaction.getCommitEntry().getTxId();
                        try
                        {
                            // TODO recovery=true needed?
                            storeApplier.apply( transaction.getTransactionRepresentation(), transactionId, true );
                            handler.accept( transaction );
                        }
                        finally
                        {
                            transactionIdStore.transactionClosed( transactionId );
                        }
                    }
                }
                return true;
            }
        } );
        return response.response();
    }

    @Override
    public void init() throws Throwable
    {

    }

    @Override
    public void start() throws Throwable
    {
        this.appender = resolver.resolveDependency( LogicalTransactionStore.class ).getAppender();
        this.storeApplier = resolver.resolveDependency( TransactionRepresentationStoreApplier.class );
        this.transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
    }

    @Override
    public void stop() throws Throwable
    {
        this.appender = null;
        this.storeApplier = null;
        this.transactionIdStore = null;
    }

    @Override
    public void shutdown() throws Throwable
    {

    }
}
