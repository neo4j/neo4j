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
package org.neo4j.impl.persistence;

import java.util.logging.Logger;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.impl.util.ArrayMap;

/**
 * The ResourceBroker is the access point for {@link ResourceConnection}s to
 * the persistence sources used. Whenever a component in the persistence layer
 * requires a persistence connection, it requests it from the ResourceBroker via
 * the {@link #acquireResourceConnection} method.
 * <P>
 * The main job of the ResourceBroker is to try to ensure that two subsequent
 * requests within the same transaction get the same connection. This not only
 * speeds up performance on our end (by enlisting/delisting fewer resources in
 * the transaction) but also saves resources in the persistence backends.
 * <P>
 * Limitations: <UL
 * <LI>Access to {@link #acquireResourceConnection} is currently serialized.
 * This may lead to monitor contention and lower performance.
 * </UL>
 */
class ResourceBroker
{
    private static Logger log = Logger.getLogger( ResourceBroker.class
        .getName() );

    private final PersistenceSourceDispatcher dispatcher;

    private ArrayMap<Transaction,ResourceConnection> txConnectionMap = 
        new ArrayMap<Transaction,ResourceConnection>( 5, true, true );

    private final TransactionManager transactionManager;

    ResourceBroker( TransactionManager transactionManager )
    {
        this.transactionManager = transactionManager;
        dispatcher = new PersistenceSourceDispatcher();
    }

    PersistenceSourceDispatcher getDispatcher()
    {
        return dispatcher;
    }

    /**
     * Acquires the resource connection that should be used to persist the
     * object represented by <CODE>meta</CODE>. This method looks up the
     * invoking thread's transaction and if the resource connection is new to
     * the transaction, the resource is enlisted according to JTA. Subsequent
     * invokations of this method with the same parameter, will return the same
     * {@link ResourceConnection}.
     * <P>
     * This method is guaranteed to never return <CODE>null</CODE>.
     * @param meta
     *            the metadata wrapper for the object that will be persisted
     *            with the returned resource connection
     * @return the {@link ResourceConnection} that should be used to persist the
     *         entity that is wrapped <CODE>meta</CODE>.
     * @throws NotInTransactionException
     *             if the resource broker is unable to fetch a transaction for
     *             the current thread
     */
    ResourceConnection acquireResourceConnection()
    {
        ResourceConnection con = null;
        PersistenceSource source = null;

        source = dispatcher.getPersistenceSource();
        Transaction tx = this.getCurrentTransaction();
        con = txConnectionMap.get( tx );
        if ( con == null )
        {
            try
            {
                con = source.createResourceConnection();
                if ( !tx.enlistResource( con.getXAResource() ) )
                {
                    throw new ResourceAcquisitionFailedException(
                        "Unable to enlist '" + con.getXAResource() + "' in "
                            + "transaction" );
                }
                tx.registerSynchronization( new TxCommitHook( tx ) );
                txConnectionMap.put( tx, con );
            }
            catch ( javax.transaction.RollbackException re )
            {
                String msg = "The transaction is marked for rollback only.";
                throw new ResourceAcquisitionFailedException( msg, re );
            }
            catch ( javax.transaction.SystemException se )
            {
                String msg = "TM encountered an unexpected error condition.";
                throw new ResourceAcquisitionFailedException( msg, se );
            }
        }
        return con;
    }

    /**
     * Releases all resources held by the transaction associated with the
     * invoking thread. If an error occurs while attempting to release a
     * resource, an error message is logged and the rest of the resources in the
     * transaction will be released.
     * @throws NotInTransactionException
     *             if the resource broker is unable to fetch a transaction for
     *             the current thread
     */
    void releaseResourceConnectionsForTransaction( Transaction tx )
        throws NotInTransactionException
    {
        ResourceConnection con = txConnectionMap.remove( tx );
        if ( con != null )
        {
            this.destroyCon( con );
        }
    }

    /**
     * Releases all resources held by the transaction associated with the
     * invoking thread.
     * @throws NotInTransactionException
     *             if the resource broker is unable to fetch a transaction for
     *             the current thread
     */
    void delistResourcesForTransaction() throws NotInTransactionException
    {
        Transaction tx = this.getCurrentTransaction();
        ResourceConnection con = txConnectionMap.get( tx );
        if ( con != null )
        {
            this.delistCon( tx, con );
        }
    }

    // Delists the XAResource that the ResourceConnection encapsulates.
    // Note that we hardcode the 'flag' to be XAResource.TMSUCCESS.
    // This code doesn't have any effect any more since resources
    // should be enlisted and delisted in xaframework implementation.
    private void delistCon( Transaction tx, ResourceConnection con )
    {
        try
        {
            tx.delistResource( con.getXAResource(), XAResource.TMSUCCESS );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            log.severe( "Failed to delist resource '" + con
                + "' from current transaction." );
            throw new RuntimeException( e );
        }
    }

    // Destroys the connection by invoking ResourceConnection.destroy(),
    // which is a hook for the resource manager to close() the underlying
    // connection (and, if pooling, return it to a connection pool).
    private void destroyCon( ResourceConnection con )
    {
        con.destroy();
    }

    // Gets the transaction associated with the currently executing thread,
    // performs sanity checks and returns it.
    private Transaction getCurrentTransaction()
        throws NotInTransactionException
    {
        try
        {
            Transaction tx = transactionManager.getTransaction();

            if ( tx == null )
            {
                throw new NotInTransactionException( "No transaction found "
                    + "for current thread" );
            }

            return tx;
        }
        catch ( SystemException se )
        {
            throw new NotInTransactionException( "Error fetching transaction "
                + "for current thread", se );
        }
    }

    private class TxCommitHook implements Synchronization
    {
        private final Transaction tx;

        TxCommitHook( Transaction tx )
        {
            this.tx = tx;
        }

        public void afterCompletion( int param )
        {
            try
            {
                releaseConnections( tx );
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                log.severe( "Unable to delist resources for tx." );
            }
        }

        public void beforeCompletion()
        {
            try
            {
                delistResourcesForTransaction();
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                log.severe( "Unable to delist resources for tx." );
            }
        }

        private void releaseConnections( Transaction tx )
        {
            try
            {
                releaseResourceConnectionsForTransaction( tx );
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                log.severe( "Error while releasing resources for tx." );
            }
        }
    }
}