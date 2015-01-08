/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import java.io.IOException;

import org.neo4j.helpers.collection.CloseableVisitor;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

public class RecoveryVisitor implements CloseableVisitor<CommittedTransactionRepresentation,IOException>
{
    public interface Monitor
    {
        void transactionRecovered( long txId );
    }

    private final TransactionIdStore store;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final Monitor monitor;
    private long lastTransactionIdApplied = -1;
    private long lastTransactionChecksum;

    public RecoveryVisitor( TransactionIdStore store,
                            TransactionRepresentationStoreApplier storeApplier, Monitor monitor )
    {
        this.store = store;
        this.storeApplier = storeApplier;
        this.monitor = monitor;
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
    {
        long txId = transaction.getCommitEntry().getTxId();
        try ( LockGroup locks = new LockGroup() )
        {
            storeApplier.apply( transaction.getTransactionRepresentation(), locks, txId,
                    TransactionApplicationMode.RECOVERY );
        }
        lastTransactionIdApplied = txId;
        lastTransactionChecksum = LogEntryStart.checksum( transaction.getStartEntry() );
        monitor.transactionRecovered( txId );
        return false;
    }

    @Override
    public void close() throws IOException
    {
        if ( lastTransactionIdApplied != -1 )
        {
            store.setLastCommittedAndClosedTransactionId( lastTransactionIdApplied, lastTransactionChecksum );
        }
    }
}
