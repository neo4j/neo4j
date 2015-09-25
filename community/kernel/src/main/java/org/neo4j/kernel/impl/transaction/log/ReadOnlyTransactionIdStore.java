/*
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStore;

import static org.neo4j.kernel.impl.store.NeoStore.Position;

public class ReadOnlyTransactionIdStore implements TransactionIdStore
{
    private final long transactionId;
    private final long transactionChecksum;
    private final long logVersion;
    private final long byteOffset;

    public ReadOnlyTransactionIdStore( PageCache pageCache, File storeDir )
    {
        long id = 0, checksum = 0, logVersion = 0, byteOffset = 0;
        if ( NeoStore.isStorePresent( pageCache, storeDir ) )
        {
            File neoStore = new File( storeDir, NeoStore.DEFAULT_NAME );
            id = NeoStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
            checksum = NeoStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
            logVersion = NeoStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            byteOffset = NeoStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        }

        this.transactionId = id;
        this.transactionChecksum = checksum;
        this.logVersion = logVersion;
        this.byteOffset = byteOffset;
    }

    @Override
    public long nextCommittingTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionCommitted( long transactionId, long checksum )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        return transactionId;
    }

    @Override
    public long[] getLastCommittedTransaction()
    {
        return new long[] {transactionId, transactionChecksum};
    }

    @Override
    public long[] getUpgradeTransaction()
    {
        return getLastCommittedTransaction();
    }

    @Override
    public long getLastClosedTransactionId()
    {
        return transactionId;
    }

    @Override
    public long[] getLastClosedTransaction()
    {
        return new long[]{transactionId, logVersion, byteOffset};
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, long checksum, long logVersion, long logByteOffset )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionClosed( long transactionId, long logVersion, long logByteOffset )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public boolean closedTransactionIdIsOnParWithOpenedTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void flush()
    {   // Nothing to flush
    }
}
