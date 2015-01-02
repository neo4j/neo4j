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

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Creates a key within its own transaction, such that the command(s) for creating the key
 * will be alone in a transaction. If there is a running a transaction while calling this
 * it will be temporarily suspended meanwhile.
 */
public abstract class IsolatedTransactionTokenCreator implements TokenCreator
{
    private final StringLogger logger;

    public IsolatedTransactionTokenCreator( Logging logging )
    {
        this.logger = logging.getMessagesLog( getClass() );
    }
    
    @Override
    public synchronized int getOrCreate( final AbstractTransactionManager txManager,
                                         final EntityIdGenerator idGenerator,
                                         final PersistenceManager persistence,
                                         final String name )
    {
        try
        {
            Transaction runningTransaction = txManager.suspend();
            try
            {
                txManager.begin( ForceMode.unforced );
                int id = createKey( idGenerator, persistence, name );
                txManager.commit();
                return id;
            }
            catch ( Throwable t )
            {
                logger.error( "Unable to create key '" + name + "'", t );
                try
                {
                    txManager.rollback();
                }
                catch ( Throwable tt )
                {
                    logger.error( "Unable to rollback after failure to create key '" + name + "'", t );
                }
                throw new TransactionFailureException( "Unable to create key '" + name + "'" , t );
            }
            finally
            {
                if ( runningTransaction != null )
                    txManager.resume( runningTransaction );
            }
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( "Unable to resume or suspend running transaction", e );
        }
        catch ( InvalidTransactionException e )
        {
            throw new TransactionFailureException( "Unable to resume or suspend running transaction", e );
        }
    }
    
    protected abstract int createKey( EntityIdGenerator idGenerator, PersistenceManager persistence, String name );
}
