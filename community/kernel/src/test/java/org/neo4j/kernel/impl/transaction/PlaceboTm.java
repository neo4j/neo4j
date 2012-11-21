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

import org.neo4j.kernel.impl.core.TransactionState;

public class PlaceboTm extends AbstractTransactionManager
{
    public void begin() throws NotSupportedException, SystemException
    {
        // TODO Auto-generated method stub

    }

    public void commit() throws RollbackException, HeuristicMixedException,
            HeuristicRollbackException, SecurityException, IllegalStateException,
            SystemException
    {
        // TODO Auto-generated method stub

    }

    public int getStatus() throws SystemException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public Transaction getTransaction() throws SystemException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void resume( Transaction arg0 ) throws InvalidTransactionException,
            IllegalStateException, SystemException
    {
        // TODO Auto-generated method stub

    }

    public void rollback() throws IllegalStateException, SecurityException,
            SystemException
    {
        // TODO Auto-generated method stub

    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        // TODO Auto-generated method stub

    }

    public void setTransactionTimeout( int arg0 ) throws SystemException
    {
        // TODO Auto-generated method stub

    }

    public Transaction suspend() throws SystemException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void init()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    @Override
    public int getEventIdentifier()
    {
        return 0;
    }

    @Override
    public void doRecovery() throws Throwable
    {
    }

    @Override
    public TransactionState getTransactionState()
    {
        return TransactionState.NO_STATE;
    }
}