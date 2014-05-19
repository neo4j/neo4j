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
package org.neo4j.kernel.impl.transaction.xaframework;

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;

public class DeadSimpleTransactionIdStore implements TransactionIdStore
{
    private long logVersion;
    private long transactionId;
    private long appliedTransactionId;

    public DeadSimpleTransactionIdStore( long initialLogVersion, long initialTransactionId )
    {
        this.logVersion = initialLogVersion;
        this.transactionId = initialTransactionId;
        this.appliedTransactionId = initialTransactionId;
    }

    @Override
    public long nextCommittingTransactionId()
    {
        return ++transactionId;
    }

    @Override
    public long getLastCommittingTransactionId()
    {
        return transactionId;
    }

    @Override
    public void transactionIdApplied( long transactionId )
    {
        appliedTransactionId = transactionId;
    }

    @Override
    public boolean appliedTransactionIsOnParWithCommittingTransactionId()
    {
        return appliedTransactionId == transactionId;
    }

    @Override
    public long nextLogVersion()
    {
        return ++logVersion;
    }

    @Override
    public long getCurrentLogVersion()
    {
        return logVersion;
    }

    @Override
    public void setCurrentLogVersion( long logVersion )
    {
        this.logVersion = logVersion;
    }

    @Override
    public void flushAll()
    {
    }
}
