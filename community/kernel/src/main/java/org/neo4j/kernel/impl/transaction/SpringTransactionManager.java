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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseSPI;

public class SpringTransactionManager implements TransactionManager
{
    private TransactionManager tm;

    /**
     * Using this constructor requires setting the the transaction manager via
     * {@link #setTransactionManager(TransactionManager)} method.
     */
    public SpringTransactionManager()
    { 
    }
    
    public SpringTransactionManager( GraphDatabaseService neo4j )
    {
        this.tm = ((GraphDatabaseSPI) neo4j).getTxManager();
    }
    
    public void begin() throws NotSupportedException, SystemException
    {
        tm.begin();
    }

    public void commit() throws RollbackException, HeuristicMixedException,
        HeuristicRollbackException, SecurityException, IllegalStateException,
        SystemException
    {
        tm.commit();
    }

    public int getStatus() throws SystemException
    {
        return tm.getStatus();
    }

    public Transaction getTransaction() throws SystemException
    {
        return tm.getTransaction();
    }

    public void resume( Transaction tx ) throws InvalidTransactionException,
        IllegalStateException, SystemException
    {
        tm.resume( tx );
    }

    public void rollback() throws IllegalStateException, SecurityException,
        SystemException
    {
        tm.rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        tm.setRollbackOnly();
    }

    public void setTransactionTimeout( int sec ) throws SystemException
    {
        tm.setTransactionTimeout( sec );
    }

    public Transaction suspend() throws SystemException
    {
        return tm.suspend();
    }

    public void setTransactionManager( TransactionManager tm )
    {
        this.tm = tm;
    }

    public TransactionManager getTransactionManager()
    {
        return tm;
    }
}