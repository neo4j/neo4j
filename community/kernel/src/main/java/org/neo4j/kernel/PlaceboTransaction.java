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
package org.neo4j.kernel;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;

public class PlaceboTransaction implements Transaction
{
    public final static Lock NO_LOCK = new Lock()
    {
        @Override
        public void release()
        {
        }
    };
    private final TransactionManager transactionManager;

    public PlaceboTransaction( TransactionManager transactionManager )
    {
        // we should override all so null is ok
        this.transactionManager = transactionManager;
    }

    public void failure()
    {
        try
        {
            transactionManager.getTransaction().setRollbackOnly();
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Failed to mark transaction as rollback only.", e );
        }
    }

    public void success()
    {
    }

    public void finish()
    {
    }
    
    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return NO_LOCK;
    }
    
    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return NO_LOCK;
    }
}
