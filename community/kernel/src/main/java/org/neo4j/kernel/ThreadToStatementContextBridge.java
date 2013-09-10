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
package org.neo4j.kernel;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link StatementOperations} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter
{
    protected final KernelAPI kernelAPI;
    private final AbstractTransactionManager txManager;
    private boolean isShutdown = false;

    public ThreadToStatementContextBridge( KernelAPI kernelAPI, AbstractTransactionManager txManager )
    {
        this.kernelAPI = kernelAPI;
        this.txManager = txManager;
    }

    public Statement statement()
    {
        return transaction().acquireStatement();
    }

    private KernelTransaction transaction()
    {
        checkIfShutdown();
        KernelTransaction transaction = txManager.getKernelTransaction();
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
        txManager.assertInTransaction();
    }
}
