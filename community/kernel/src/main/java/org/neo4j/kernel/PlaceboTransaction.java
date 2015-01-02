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
package org.neo4j.kernel;

import org.neo4j.graphdb.Lock;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class PlaceboTransaction extends TopLevelTransaction
{
    public final static Lock NO_LOCK = new Lock()
    {
        @Override
        public void release()
        {
        }
    };
    
    public PlaceboTransaction( PersistenceManager pm, AbstractTransactionManager transactionManager, TransactionState state )
    {
        super( pm, transactionManager, state );
    }

    @Override
    public void close()
    {
        if ( !transactionOutcome.successCalled() && !transactionOutcome.failureCalled() )
        {
            markAsRollbackOnly();
        }
    }
}
