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
package org.neo4j.kernel.impl.core;

import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

public class WritableTransactionState implements TransactionState
{
    // Dependencies
    private final RemoteTxHook txHook;
    private final TxIdGenerator txIdGenerator;

    private boolean isRemotelyInitialized = false;

    public WritableTransactionState( RemoteTxHook txHook, TxIdGenerator txIdGenerator )
    {
        this.txHook = txHook;
        this.txIdGenerator = txIdGenerator;
    }

    @Override
    public RemoteTxHook getTxHook()
    {
        return txHook;
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return txIdGenerator;
    }

    @Override
    public boolean isRemotelyInitialized()
    {
        return isRemotelyInitialized;
    }

    @Override
    public void markAsRemotelyInitialized()
    {
        isRemotelyInitialized = true;
    }
}
