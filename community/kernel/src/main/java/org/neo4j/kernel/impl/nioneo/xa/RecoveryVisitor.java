/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;

public class RecoveryVisitor implements Visitor<CommittedTransactionRepresentation, IOException>, Closeable
{
    private final TransactionIdStore store;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final AtomicInteger recoveredCount;

    private long lastTransactionIdApplied = -1;

    public RecoveryVisitor( TransactionIdStore store,
                            TransactionRepresentationStoreApplier storeApplier,
                            AtomicInteger recoveredCount )
    {
        this.store = store;
        this.storeApplier = storeApplier;
        this.recoveredCount = recoveredCount;
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
    {
        long txId = transaction.getCommitEntry().getTxId();
        storeApplier.apply( transaction.getTransactionRepresentation(), txId, true );
        recoveredCount.incrementAndGet();
        lastTransactionIdApplied = txId;
        return true;
    }

    @Override
    public void close() throws IOException
    {
        if ( lastTransactionIdApplied != -1 )
        {
            store.setLastCommittingAndClosedTransactionId( lastTransactionIdApplied );
        }
    }
}
