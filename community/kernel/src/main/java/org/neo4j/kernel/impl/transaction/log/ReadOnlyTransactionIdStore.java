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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;

public class ReadOnlyTransactionIdStore implements TransactionIdStore
{
    private final long transactionId;
    private final long transactionChecksum;

    public ReadOnlyTransactionIdStore( FileSystemAbstraction fs, File storeDir )
    {
        long id = 0, checksum = 0;
        if ( NeoStoreUtil.neoStoreExists( fs, storeDir ) )
        {
            NeoStoreUtil access = new NeoStoreUtil( storeDir, fs );
            id = access.getLastCommittedTx();
            checksum = access.getLastCommittedTxChecksum();
        }

        this.transactionId = id;
        this.transactionChecksum = checksum;
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
    public void setLastCommittedAndClosedTransactionId( long transactionId, long checksum )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionClosed( long transactionId )
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
