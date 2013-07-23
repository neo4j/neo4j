/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.operations.StatementState;
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
    public void begin( ForceMode forceMode ) throws NotSupportedException, SystemException
    {
        begin();
    }

    public abstract void doRecovery() throws Throwable;

    /**
     * @return which {@link ForceMode} the transaction tied to the calling
     *         thread will have when committing. Default is {@link ForceMode#forced}
     */
    public ForceMode getForceMode()
    {
        return ForceMode.forced;
    }
    
    /**
     * Returns the {@link TransactionState} associated with the current transaction.
     * If no transaction is active for the current thread {@link TransactionState#NO_STATE}
     * should be returned.
     * 
     * @return state associated with the current transaction for this thread.
     */
    public abstract TransactionState getTransactionState();

    public abstract int getEventIdentifier();

    /**
     * @return the error that happened during recovery, if recovery has taken place, null otherwise.
     */
    public Throwable getRecoveryError()
    {
        return null;
    }

    /**
     * Temporarily here during transition to Kernel API.
     *
     * @return an open statement context for the current transaction. If none exists, it should create one. It
     *         can do this using the kernel api that is provided at startup through the (ick) setKernel method.
     */
    @Deprecated
    public StatementState newStatement()
    {
        throw new UnsupportedOperationException( "The current transaction manager implementation does not support the " +
                "new StatementContext interface. This is an intermediary problem during transition to a new internal API." );
    }
    
    /**
     * Temporarily here during transition to Kernel API
     * @param kernel
     */
    @Deprecated
    public abstract void setKernel( KernelAPI kernel );
    
    /**
     * Temporarily here during transition to Kernel API.
     *
     * @return an open transaction context for the current transaction. If none exists, it should create one. It
     *         can do this using the kernel api that is provided at startup through the (ick) setKernel method.
     */
    @Deprecated
    public KernelTransaction getKernelTransaction()
    {
        throw new UnsupportedOperationException( "The current transaction manager implementation does not support the " +
                "new TransactionContext interface. This is an intermediary problem during transition to a new internal API." );
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
