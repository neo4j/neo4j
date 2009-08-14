/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.impl.util.ArrayMap;

public class PersistenceManager
{
    private static Logger log = Logger.getLogger( PersistenceManager.class
        .getName() );
    
    private final PersistenceSource persistenceSource;
    private final TransactionManager transactionManager;
    
    private final ArrayMap<Transaction,ResourceConnection> txConnectionMap = 
        new ArrayMap<Transaction,ResourceConnection>( 5, true, true );
    
    public PersistenceManager( TransactionManager transactionManager, 
        PersistenceSource persistenceSource )
    {
        this.transactionManager = transactionManager;
        this.persistenceSource = persistenceSource;
    }
    
    public PersistenceSource getPersistenceSource()
    {
        return persistenceSource;
    }

    public boolean loadLightNode( int id )
    {
        return getReadOnlyResource().nodeLoadLight( id );
    }

    public Object loadPropertyValue( int id )
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

    public Iterable<RelationshipData> loadRelationships( int nodeId )
    {
        return getReadOnlyResource().nodeLoadRelationships( nodeId );
    }

    public ArrayMap<Integer,PropertyData> loadNodeProperties( int nodeId )
    {
        return getReadOnlyResource().nodeLoadProperties( nodeId );
    }

    public ArrayMap<Integer,PropertyData> loadRelProperties( int relId )
    {
        return getReadOnlyResource().relLoadProperties( relId );
    }

    public RelationshipData loadLightRelationship( int id )
    {
        return getReadOnlyResource().relLoadLight( id );
    }

    public RelationshipTypeData[] loadAllRelationshipTypes()
    {
        return getReadOnlyResource().loadRelationshipTypes();
    }

    public void nodeDelete( int nodeId )
    {
        getResource().nodeDelete( nodeId );
    }

    public int nodeAddProperty( int nodeId, PropertyIndex index, Object value )
    {
        return getResource().nodeAddProperty( nodeId, index, value );
    }

    public void nodeChangeProperty( int nodeId, int propertyId, Object value )
    {
        getResource().nodeChangeProperty( nodeId, propertyId, value );
    }

    public void nodeRemoveProperty( int nodeId, int propertyId )
    {
        getResource().nodeRemoveProperty( nodeId, propertyId );
    }

    public void nodeCreate( int id )
    {
        getResource().nodeCreate( id );
    }

    public void relationshipCreate( int id, int typeId, int startNodeId,
        int endNodeId )
    {
        getResource().relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public void relDelete( int relId )
    {
        getResource().relDelete( relId );
    }

    public int relAddProperty( int relId, PropertyIndex index, Object value )
    {
        return getResource().relAddProperty( relId, index, value );
    }

    public void relChangeProperty( int relId, int propertyId, Object value )
    {
        getResource().relChangeProperty( relId, propertyId, value );
    }

    public void relRemoveProperty( int relId, int propertyId )
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
            return ((NioNeoDbPersistenceSource) 
                persistenceSource ).createReadOnlyResourceConnection();
        }
        return con;
    }
    
    private ResourceConnection getResource()
    {
        ResourceConnection con = null;

        Transaction tx = this.getCurrentTransaction();
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

    void delistResourcesForTransaction() throws NotInTransactionException
    {
        Transaction tx = this.getCurrentTransaction();
        ResourceConnection con = txConnectionMap.get( tx );
        if ( con != null )
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
}