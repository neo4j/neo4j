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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;

public class ReadOnlyTransactionIdStore implements TransactionIdStore
{
    private final long transactionId;

    public ReadOnlyTransactionIdStore( FileSystemAbstraction fs, File storeDir )
    {
        this.transactionId = NeoStoreUtil.neoStoreExists( fs, storeDir ) ?
                new NeoStoreUtil( storeDir, fs ).getLastCommittedTx() : 0;
    }

    @Override
    public long nextCommittingTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionCommitted( long transactionId )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }
    
    @Override
    public long getLastCommittedTransactionId()
    {
        return transactionId;
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void transactionClosed( long transactionId )
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public boolean closedTransactionIdIsOnParWithCommittedTransactionId()
    {
        throw new UnsupportedOperationException( "Read-only transaction ID store" );
    }

    @Override
    public void flush()
    {   // Nothing to flush
    }
}
