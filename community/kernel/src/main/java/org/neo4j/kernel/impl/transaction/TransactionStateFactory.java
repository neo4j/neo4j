/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.logging.Logging;

public class TransactionStateFactory
{
    private LockManager lockManager;
    private PropertyIndexManager propertyIndexManager;
    private NodeManager nodeManager;
    private final Logging logging;
    
    public TransactionStateFactory( Logging logging )
    {
        this.logging = logging;
    }
    
    public void setDependencies( LockManager lockManager, PropertyIndexManager propertyIndexManager,
            NodeManager nodeManager )
    {
        this.lockManager = lockManager;
        this.propertyIndexManager = propertyIndexManager;
        this.nodeManager = nodeManager;
    }
    
    public TransactionState create( Transaction tx )
    {
        return new WritableTransactionState( lockManager, propertyIndexManager, nodeManager,
                logging, tx );
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
