/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.persistence;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipChainPosition;
import org.neo4j.kernel.impl.nioneo.store.RelationshipData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

public class PersistenceManager
{
    private static Logger log = Logger.getLogger( PersistenceManager.class
        .getName() );
    
    private final PersistenceSource persistenceSource;
    private final TransactionManager transactionManager;
    
    private final ArrayMap<Transaction,ResourceConnection> txConnectionMap = 
        new ArrayMap<Transaction,ResourceConnection>( 5, true, true );

    private final TxEventSyncHookFactory syncHookFactory;

    public PersistenceManager( TransactionManager transactionManager, 
        PersistenceSource persistenceSource, TxEventSyncHookFactory syncHookFactory )
    {
        this.transactionManager = transactionManager;
        this.persistenceSource = persistenceSource;
        this.syncHookFactory = syncHookFactory;
    }
    
    public PersistenceSource getPersistenceSource()
    {
        return persistenceSource;
    }

    public boolean loadLightNode( long id )
    {
        return getReadOnlyResource().nodeLoadLight( id );
    }

    public Object loadPropertyValue( long id )
    {
        return getReadOnlyResource().loadPropertyValue( id );
    }

    public String loadIndex( int id )
    {
        return getReadOnlyResource().loadIndex( id );
    }

    public PropertyIndexData[] loadPropertyIndexes( int maxCount )
    {
        return getReadOnlyResource().loadPropertyIndexes( maxCount );
    }

    public RelationshipChainPosition getRelationshipChainPosition( long nodeId )
    {
        return getReadOnlyResource().getRelationshipChainPosition( nodeId );
    }
    
    public Iterable<RelationshipData> getMoreRelationships( long nodeId,
        RelationshipChainPosition position )
    {
        return getReadOnlyResource().getMoreRelationships( nodeId, position );
    }
    
    public ArrayMap<Integer,PropertyData> loadNodeProperties( long nodeId,
            boolean light )
    {
        return getReadOnlyResource().nodeLoadProperties( nodeId, light );
    }

    public ArrayMap<Integer,PropertyData> loadRelProperties( long relId,
            boolean light )
    {
        return getReadOnlyResource().relLoadProperties( relId, light );
    }

    public RelationshipData loadLightRelationship( long id )
    {
        return getReadOnlyResource().relLoadLight( id );
    }

    public RelationshipTypeData[] loadAllRelationshipTypes()
    {
        return getReadOnlyResource().loadRelationshipTypes();
    }

    public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
    {
        return getResource().nodeDelete( nodeId );
    }

    public long nodeAddProperty( long nodeId, PropertyIndex index, Object value )
    {
        return getResource().nodeAddProperty( nodeId, index, value );
    }

    public void nodeChangeProperty( long nodeId, long propertyId, Object value )
    {
        getResource().nodeChangeProperty( nodeId, propertyId, value );
    }

    public void nodeRemoveProperty( long nodeId, long propertyId )
    {
        getResource().nodeRemoveProperty( nodeId, propertyId );
    }

    public void nodeCreate( long id )
    {
        getResource().nodeCreate( id );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId,
        long endNodeId )
    {
        getResource().relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public ArrayMap<Integer,PropertyData> relDelete( long relId )
    {
        return getResource().relDelete( relId );
    }

    public long relAddProperty( long relId, PropertyIndex index, Object value )
    {
        return getResource().relAddProperty( relId, index, value );
    }

    public void relChangeProperty( long relId, long propertyId, Object value )
    {
        getResource().relChangeProperty( relId, propertyId, value );
    }

    public void relRemoveProperty( long relId, long propertyId )
    {
        getResource().relRemoveProperty( relId, propertyId );
    }

    public void createPropertyIndex( String key, int id )
    {
        getResource().createPropertyIndex( key, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource().createRelationshipType( id, name );
    }

    private ResourceConnection getReadOnlyResource()
    {
        Transaction tx = this.getCurrentTransaction();
        ResourceConnection con = txConnectionMap.get( tx );
        if ( con == null )
        {
            // con is put in map on write operation, see getResoure()
            // createReadOnlyResourceConnection just return a single final 
            // resource and does not create a new object
            return ((NioNeoDbPersistenceSource) 
                persistenceSource ).createReadOnlyResourceConnection();
        }
        return con;
    }
    
    private ResourceConnection getResource()
    {
        ResourceConnection con = null;

        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        con = txConnectionMap.get( tx );
        if ( con == null )
        {
            try
            {
                con = persistenceSource.createResourceConnection();
                if ( !tx.enlistResource( con.getXAResource() ) )
                {
                    throw new ResourceAcquisitionFailedException(
                        "Unable to enlist '" + con.getXAResource() + "' in "
                            + "transaction" );
                }
                
                tx.registerSynchronization( new TxCommitHook( tx ) );
                registerTransactionEventHookIfNeeded();
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
    
    private void registerTransactionEventHookIfNeeded()
            throws SystemException, RollbackException
    {
        TransactionEventsSyncHook hook = syncHookFactory.create();
        if ( hook != null )
        {
            this.transactionManager.getTransaction().registerSynchronization(
                    hook );
        }
    }
    
    private Transaction getCurrentTransaction()
        throws NotInTransactionException
    {
        try
        {
            return transactionManager.getTransaction();
        }
        catch ( SystemException se )
        {
            throw new TransactionFailureException( "Error fetching transaction "
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
                log.log( Level.SEVERE, 
                    "Unable to release connections for " + tx, t );
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
                log.log( Level.SEVERE, 
                    "Unable to delist resources for " + tx, t );
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
                log.log( Level.SEVERE, 
                    "Error releasing resources for " + tx, t );
            }
        }
    }

    void delistResourcesForTransaction() throws NotInTransactionException
    {
        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        ResourceConnection con = txConnectionMap.get( tx );
        if ( con != null )
        {
            try
            {
                tx.delistResource( con.getXAResource(), XAResource.TMSUCCESS );
            }
            catch ( SystemException e )
            {
                throw new TransactionFailureException(
                    "Failed to delist resource '" + con + 
                    "' from current transaction.", e );
            }
        }
    }
    
    void releaseResourceConnectionsForTransaction( Transaction tx )
        throws NotInTransactionException
    {
        ResourceConnection con = txConnectionMap.remove( tx );
        if ( con != null )
        {
            con.destroy();
        }
    }

    public RelIdArray getCreatedNodes()
    {
        return getResource().getCreatedNodes();
    }

    public boolean isNodeCreated( long nodeId )
    {
        return getResource().isNodeCreated( nodeId );
    }

    public boolean isRelationshipCreated( long relId )
    {
        return getResource().isRelationshipCreated( relId );
    }

    public int getKeyIdForProperty( long propertyId )
    {
        return getReadOnlyResource().getKeyIdForProperty( propertyId );
    }

}