/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.util.Access;

/**
 * Queues {@link TransactionRepresentation} for application at a later point. Queued transactions can be visited
 * with {@link #accept(TransactionVisitor)} where the transactions are intact in the queue after that call.
 */
public class TransactionQueue
{
    private final Transaction[] queue;
    private int queueIndex;

    public TransactionQueue( int threshold )
    {
        this.queue = new Transaction[threshold];
        for ( int i = 0; i < threshold; i++ )
        {
            this.queue[i] = new Transaction();
        }
    }

    public boolean queue( CommittedTransactionRepresentation transaction, TxHandler txHandler )
    {
        assert queueIndex < queue.length : "Tried to queue beyond capacity, qIndex " + queueIndex;
        queue[queueIndex++].set( transaction, txHandler );
        return queueIndex >= queue.length;
    }

    public int accept( TransactionVisitor visitor )
            throws IOException
    {
        for ( int i = 0; i < queueIndex; i++ )
        {
            Transaction tx = queue[i];
            visitor.visit( tx.transaction, tx.txHandler, tx );
        }
        return queueIndex;
    }

    public void clear()
    {
        queueIndex = 0;
    }

    public CommittedTransactionRepresentation last()
    {
        if ( isEmpty() )
        {
            throw new IllegalStateException( "Nothing in queue" );
        }
        return queue[queueIndex - 1].transaction;
    }

    public boolean isEmpty()
    {
        return queueIndex == 0;
    }

    public CommittedTransactionRepresentation first()
    {
        if ( isEmpty() )
        {
            throw new IllegalStateException( "Nothing in queue" );
        }
        return queue[0].transaction;
    }

    private static class Transaction implements Access<Commitment>
    {
        private CommittedTransactionRepresentation transaction;
        private TxHandler txHandler;
        private Commitment commitment;

        void set( CommittedTransactionRepresentation transaction, TxHandler txHandler )
        {
            this.transaction = transaction;
            this.txHandler = txHandler;
            this.commitment = null; // null at this point. Access provides means of setting it
        }

        @Override
        public Commitment get()
        {
            return commitment;
        }

        @Override
        public void set( Commitment commitment )
        {
            this.commitment = commitment;
        }
    }
}
