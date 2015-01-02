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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link Statement} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter implements Provider<Statement>
{
    private final PersistenceManager persistenceManager;
    private boolean isShutdown = false;

    public ThreadToStatementContextBridge( PersistenceManager persistenceManager )
    {
        this.persistenceManager = persistenceManager;
    }

    @Override
    public Statement instance()
    {
        return transaction().acquireStatement();
    }

    private KernelTransaction transaction()
    {
        checkIfShutdown();
        KernelTransaction transaction = persistenceManager.currentKernelTransactionForReading();
        if ( transaction == null )
        {
            throw new NotInTransactionException();
        }
        return transaction;
    }

    @Override
    public void shutdown() throws Throwable
    {
        isShutdown = true;
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }

    public void assertInTransaction()
    {
        checkIfShutdown();
        // Contract: Persistence manager throws NotInTransactionException if we are not in a transaction.
        persistenceManager.getCurrentTransaction();
    }
}
