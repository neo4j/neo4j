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

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class ResponsePacker
{
    protected final LogicalTransactionStore transactionStore;
    protected final Provider<StoreId> storeId; // for lazy storeId getter
    private final TransactionIdStore transactionIdStore;

    public ResponsePacker( LogicalTransactionStore transactionStore, TransactionIdStore transactionIdStore,
            Provider<StoreId> storeId )
    {
        this.transactionStore = transactionStore;
        this.transactionIdStore = transactionIdStore;
        this.storeId = storeId;
    }

    public <T> Response<T> packResponse( RequestContext context, T response )
    {
        return packResponse( context, response, Predicates.<CommittedTransactionRepresentation>TRUE() );
    }

    public <T> Response<T> packResponse( RequestContext context, T response,
            final Predicate<CommittedTransactionRepresentation> filter )
    {
        final long toStartFrom = context.lastAppliedTransaction() + 1;
        TransactionStream transactions = new TransactionStream()
        {
            @Override
            public void accept( Visitor<CommittedTransactionRepresentation, IOException> visitor ) throws IOException
            {
                if ( toStartFrom > BASE_TX_ID && toStartFrom <= transactionIdStore.getLastCommittedTransactionId() )
                {
                    extractTransactions( toStartFrom, filterVisitor( visitor, filter ) );
                }
            }
        };
        return new Response<>( response, db.storeId(), transactions, ResourceReleaser.NO_OP );
    }

    protected Visitor<CommittedTransactionRepresentation, IOException> filterVisitor(
            final Visitor<CommittedTransactionRepresentation, IOException> delegate,
            final Predicate<CommittedTransactionRepresentation> filter )
    {
        return new Visitor<CommittedTransactionRepresentation, IOException>()
        {
            @Override
            public boolean visit( CommittedTransactionRepresentation element ) throws IOException
            {
                return !filter.accept( element ) || delegate.visit( element );
            }
        };
    }

    protected void extractTransactions( long startingAtTransactionId,
            Visitor<CommittedTransactionRepresentation,IOException> visitor ) throws IOException
    {
        try (IOCursor<CommittedTransactionRepresentation> cursor = transactionStore.getTransactions( startingAtTransactionId) )
        {
            while (cursor.next() && visitor.visit( cursor.get() ));
        }
    }
}
