/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import java.util.function.Supplier;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class ResponsePacker
{
    protected final LogicalTransactionStore transactionStore;
    protected final Supplier<StoreId> storeId; // for lazy storeId getter
    private final TransactionIdStore transactionIdStore;

    public ResponsePacker( LogicalTransactionStore transactionStore, TransactionIdStore transactionIdStore,
                           Supplier<StoreId> storeId )
    {
        this.transactionStore = transactionStore;
        this.transactionIdStore = transactionIdStore;
        this.storeId = storeId;
    }

    public <T> Response<T> packTransactionStreamResponse( RequestContext context, T response )
    {
        final long toStartFrom = context.lastAppliedTransaction() + 1;
        final long toEndAt = transactionIdStore.getLastCommittedTransactionId();
        TransactionStream transactions = visitor ->
        {
            // Check so that it's even worth thinking about extracting any transactions at all
            if ( toStartFrom > BASE_TX_ID && toStartFrom <= toEndAt )
            {
                extractTransactions( toStartFrom, filterVisitor( visitor, toEndAt ) );
            }
        };
        return new TransactionStreamResponse<>( response, storeId.get(), transactions, ResourceReleaser.NO_OP );
    }

    public <T> Response<T> packTransactionObligationResponse( RequestContext context, T response )
    {
        return packTransactionObligationResponse( context, response,
                transactionIdStore.getLastCommittedTransactionId() );
    }

    public <T> Response<T> packTransactionObligationResponse( RequestContext context, T response,
                                                              long obligationTxId )
    {
        return new TransactionObligationResponse<>( response, storeId.get(), obligationTxId,
                ResourceReleaser.NO_OP );
    }

    public <T> Response<T> packEmptyResponse( T response )
    {
        return new TransactionObligationResponse<>( response, storeId.get(), TransactionIdStore.BASE_TX_ID,
                ResourceReleaser.NO_OP );
    }

    protected Visitor<CommittedTransactionRepresentation,Exception> filterVisitor(
            final Visitor<CommittedTransactionRepresentation,Exception> delegate, final long txToEndAt )
    {
        return element -> element.getCommitEntry().getTxId() <= txToEndAt && delegate.visit( element );
    }

    protected void extractTransactions( long startingAtTransactionId,
                                        Visitor<CommittedTransactionRepresentation,Exception> visitor )
            throws Exception
    {
        try ( IOCursor<CommittedTransactionRepresentation> cursor = transactionStore
                .getTransactions( startingAtTransactionId ) )
        {
            while ( cursor.next() && !visitor.visit( cursor.get() ) )
            {
            }
        }
    }
}
