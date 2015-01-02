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
package org.neo4j.kernel.impl.transaction;

import javax.transaction.Transaction;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.logging.Logging;

public class TransactionStateFactory
{
    protected LockManager lockManager;
    protected NodeManager nodeManager;
    protected final Logging logging;
    protected RemoteTxHook txHook;
    protected TxIdGenerator txIdGenerator;
    
    public TransactionStateFactory( Logging logging )
    {
        this.logging = logging;
    }
    
    public void setDependencies( LockManager lockManager,
            NodeManager nodeManager, RemoteTxHook txHook, TxIdGenerator txIdGenerator )
    {
        this.lockManager = lockManager;
        this.nodeManager = nodeManager;
        this.txHook = txHook;
        this.txIdGenerator = txIdGenerator;
    }
    
    public TransactionState create( Transaction tx )
    {
        return new WritableTransactionState( lockManager, nodeManager,
                logging, tx, txHook, txIdGenerator );
    }
    
    public static TransactionStateFactory noStateFactory( Logging logging )
    {
        return new TransactionStateFactory( logging )
        {
            @Override
            public TransactionState create( Transaction tx )
            {
                return TransactionState.NO_STATE;
            }
        };
    }
}
