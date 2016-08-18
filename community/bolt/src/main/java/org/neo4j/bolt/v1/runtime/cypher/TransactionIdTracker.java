/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.cypher;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.neo4j.function.Predicates.tryAwait;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

class TransactionIdTracker implements VersionTracker
{
    private final Supplier<TransactionIdStore> transactionIdStore;
    private final int timeout;
    private final TimeUnit timeoutUnit;

    private long txId;

    TransactionIdTracker( Supplier<TransactionIdStore> transactionIdStore, long transactionId, int timeout,
            TimeUnit unit )
    {
        this.transactionIdStore = transactionIdStore;
        this.txId = transactionId;
        this.timeout = timeout;
        this.timeoutUnit = unit;
    }

    @Override
    public void assertUpToDate() throws TransactionFailureException
    {
        if ( txId <= BASE_TX_ID )
        {
            return;
        }

        try
        {
            if ( !tryAwait( () -> txId <= transactionIdStore.get().getLastClosedTransactionId(), timeout, timeoutUnit,
                    25, TimeUnit.MILLISECONDS ) )
            {
                throw new TransactionFailureException( Status.Transaction.InstanceStateChanged,
                        "Database not up to the requested version: %d. Latest database version is %d", txId,
                        transactionIdStore.get().getLastClosedTransactionId() );
            }
        }
        catch ( InterruptedException e )
        {
            throw new TransactionFailureException( Status.Transaction.TransactionStartFailed, e,
                    "Thread interrupted when starting transaction" );
        }
    }

    @Override
    public void updateVersion( long version )
    {
        if ( txId < 0 )
        {
            return;
        }

        this.txId = version <= BASE_TX_ID ? transactionIdStore.get().getLastClosedTransactionId() : version;
    }
}
