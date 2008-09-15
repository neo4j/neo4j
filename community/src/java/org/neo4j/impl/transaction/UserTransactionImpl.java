/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

public class UserTransactionImpl implements UserTransaction
{
    private static final UserTransactionImpl instance = new UserTransactionImpl();

    static TransactionManager tm = null;

    public static UserTransactionImpl getInstance()
    {
        return instance;
    }

    private UserTransactionImpl()
    {
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

    public void rollback() throws SecurityException, IllegalStateException,
        SystemException
    {
        tm.rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        tm.setRollbackOnly();
    }

    public int getStatus() throws SystemException
    {
        return tm.getStatus();
    }

    public void setTransactionTimeout( int seconds ) throws SystemException
    {
        tm.setTransactionTimeout( seconds );
    }

    /**
     * Returns the event identifier for the current transaction. If no
     * transaction is active <CODE>null</CODE> is returned.
     */
    public Integer getEventIdentifier()
    {
        try
        {
            TransactionImpl tx = (TransactionImpl) tm.getTransaction();
            if ( tx != null )
            {
                return tx.getEventIdentifier();
            }
        }
        catch ( SystemException e )
        {
        }
        return null;
    }
}