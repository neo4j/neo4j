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

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * This interface extends the TransactionManager, with the rationale that it
 * additionally provides an init method that is used for recovery and a stop
 * method for shutting down. Implementations are to hold an actual
 * TransactionManager and forward operations to it and additionally provide an
 * implementation specific way of initializing it, ensuring tx recovery and an
 * implementation specific way of shutting down, for resource reclamation.
 *
 * @author Chris Gioran
 */
public abstract class AbstractTransactionManager implements TransactionManager, Lifecycle
{
    public abstract void doRecovery() throws Throwable;

    /**
     * Returns the {@link TransactionState} associated with the current transaction.
     * If no transaction is active for the current thread {@link TransactionState#NO_STATE}
     * should be returned.
     *
     * @return state associated with the current transaction for this thread.
     */
    public abstract TransactionState getTransactionState();

    public abstract int getEventIdentifier();

    public void begin( ForceMode forceMode ) throws NotSupportedException, SystemException
    {
        begin();
    }

    /**
     * @return which {@link ForceMode} the transaction tied to the calling
     *         thread will have when committing. Default is {@link ForceMode#forced}
     */
    public ForceMode getForceMode()
    {
        return ForceMode.forced;
    }

    /**
     * @return the error that happened during recovery, if recovery has taken place, null otherwise.
     */
    public Throwable getRecoveryError()
    {
        return null;
    }

    public void assertInTransaction()
    {
        try
        {
            if ( getTransaction() == null )
            {
                throw new NotInTransactionException();
            }
        }
        catch ( SystemException e )
        {
            throw new IllegalStateException( "Unable to determine transaction state", e );
        }
    }
}
