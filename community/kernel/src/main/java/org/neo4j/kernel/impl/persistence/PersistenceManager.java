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
package org.neo4j.kernel.impl.persistence;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

public class PersistenceManager
{
    private static Logger log = Logger.getLogger( PersistenceManager.class
        .getName() );

    private final PersistenceSource persistenceSource;
    private final AbstractTransactionManager transactionManager;

    private final ArrayMap<Transaction,NeoStoreTransaction> txConnectionMap =
        new ArrayMap<Transaction,NeoStoreTransaction>( (byte)5, true, true );

    private final TxEventSyncHookFactory syncHookFactory;

    public PersistenceManager( AbstractTransactionManager transactionManager,
            PersistenceSource persistenceSource,
            TxEventSyncHookFactory syncHookFactory )
    {
        this.transactionManager = transactionManager;
        this.persistenceSource = persistenceSource;
        this.syncHookFactory = syncHookFactory;
    }

    public NodeRecord loadLightNode( long id )
    {
        return getReadOnlyResourceIfPossible().nodeLoadLight( id );
    }

    public Object loadPropertyValue( PropertyData property )
    {
        return getReadOnlyResource().loadPropertyValue( property );
    }

    public String loadIndex( int id )
    {
        return getReadOnlyResourceIfPossible().loadIndex( id );
    }

    public NameData[] loadPropertyIndexes( int maxCount )
    {
        /* Using getReadOnlyResource here since loadPropertyIndexes doesn't
         * follow transaction state visibility standard anyway. There's also
         * an issue where this is called right after the neostore data source
         * has been started, but potentially before all recovery has been
         * done on all data sources, meaning that a call to getTransaction
         * could fail.
         */
        return getReadOnlyResource/*IfPossible*/().loadPropertyIndexes( maxCount );
    }

    public long getRelationshipChainPosition( long nodeId )
    {
        return getReadOnlyResourceIfPossible().getRelationshipChainPosition( nodeId );
    }

    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position )
    {
        return getReadOnlyResource().getMoreRelationships( nodeId, position );
    }

    public ArrayMap<Integer,PropertyData> loadNodeProperties( long nodeId, boolean light )
    {
        return getReadOnlyResourceIfPossible().nodeLoadProperties( nodeId, light );
    }

    public ArrayMap<Integer,PropertyData> loadRelProperties( long relId,
            boolean light )
    {
        return getReadOnlyResourceIfPossible().relLoadProperties( relId, light );
    }
    
    public RelationshipRecord loadLightRelationship( long id )
    {
        return getReadOnlyResourceIfPossible().relLoadLight( id );
    }

    public NameData[] loadAllRelationshipTypes()
    {
        /* Using getReadOnlyResource here since loadRelationshipTypes doesn't
         * follow transaction state visibility standard anyway. There's also
         * an issue where this is called right after the neostore data source
         * has been started, but potentially before all recovery has been
         * done on all data sources, meaning that a call to getTransaction
         * could fail.
         */
        return getReadOnlyResource/*IfPossible*/().loadRelationshipTypes();
    }

    public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
    {
        return getResource( true ).nodeDelete( nodeId );
    }

    public PropertyData nodeAddProperty( long nodeId, PropertyIndex index, Object value )
    {
        return getResource( true ).nodeAddProperty( nodeId, index, value );
    }

    public PropertyData nodeChangeProperty( long nodeId, PropertyData data,
            Object value )
    {
        return getResource( true ).nodeChangeProperty( nodeId, data, value );
    }

    public void nodeRemoveProperty( long nodeId, PropertyData data )
    {
        getResource( true ).nodeRemoveProperty( nodeId, data );
    }

    public void nodeCreate( long id )
    {
        getResource( true ).nodeCreate( id );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId,
        long endNodeId )
    {
        getResource( true ).relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public ArrayMap<Integer,PropertyData> relDelete( long relId )
    {
        return getResource( true ).relDelete( relId );
    }

    public PropertyData relAddProperty( long relId, PropertyIndex index, Object value )
    {
        return getResource( true ).relAddProperty( relId, index, value );
    }

    public PropertyData relChangeProperty( long relId, PropertyData data,
            Object value )
    {
        return getResource( true ).relChangeProperty( relId, data, value );
    }

    public void relRemoveProperty( long relId, PropertyData data )
    {
        getResource( true ).relRemoveProperty( relId, data );
    }

    public PropertyData graphAddProperty( PropertyIndex index, Object value )
    {
        return getResource( true ).graphAddProperty( index, value );
    }

    public PropertyData graphChangeProperty( PropertyData data, Object value )
    {
        return getResource( true ).graphChangeProperty( data, value );
    }

    public void graphRemoveProperty( PropertyData data )
    {
        getResource( true ).graphRemoveProperty( data );
    }
    
    public ArrayMap<Integer, PropertyData> graphLoadProperties( boolean light )
    {
        return getReadOnlyResourceIfPossible().graphLoadProperties( light );
    }
    
    public void createPropertyIndex( String key, int id )
    {
        getResource( true ).createPropertyIndex( key, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource( false ).createRelationshipType( id, name );
    }

    private NeoStoreTransaction getReadOnlyResource()
    {
        return ((NioNeoDbPersistenceSource)
                persistenceSource ).createReadOnlyResourceConnection();
    }

    private NeoStoreTransaction getReadOnlyResourceIfPossible()
    {
        Transaction tx = this.getCurrentTransaction();
//        if ( tx == null )
//        {
//            return ((NioNeoDbPersistenceSource)
//                    persistenceSource ).createReadOnlyResourceConnection();
//        }

        NeoStoreTransaction con = txConnectionMap.get( tx );
        if ( con == null )
        {
            // con is put in map on write operation, see getResoure()
            // createReadOnlyResourceConnection just return a single final
            // resource and does not create a new object
            /*
             * return ((NioNeoDbPersistenceSource)
                persistenceSource ).createReadOnlyResourceConnection();
             */
            return getReadOnlyResource();
        }
        return con;
    }

    private NeoStoreTransaction getResource( boolean registerEventHooks )
    {
        NeoStoreTransaction con = null;

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
                XaConnection xaConnection = persistenceSource.getXaDataSource().getXaConnection();
                XAResource xaResource = xaConnection.getXaResource();
                if ( !tx.enlistResource( xaResource ) )
                {
                    throw new ResourceAcquisitionFailedException(
                        "Unable to enlist '" + xaResource + "' in " + "transaction" );
                }
                con = persistenceSource.createTransaction( xaConnection );

                TransactionState state = transactionManager.getTransactionState();
                tx.registerSynchronization( new TxCommitHook( tx, state ) );
                if ( registerEventHooks )
                    registerTransactionEventHookIfNeeded( tx );
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

    private void registerTransactionEventHookIfNeeded( Transaction tx )
            throws SystemException, RollbackException
    {
        TransactionEventsSyncHook hook = syncHookFactory.create();
        if ( hook != null )
        {
            tx.registerSynchronization( hook );
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
        private final TransactionState state;

        TxCommitHook( Transaction tx, TransactionState state )
        {
            this.tx = tx;
            this.state = state;
        }

        @Override
        public void afterCompletion( int param )
        {
            releaseConnections( tx );
            if ( param == Status.STATUS_COMMITTED )
            {
                state.commit();
            }
            else
            {
                state.rollback();
            }
        }

        @Override
        public void beforeCompletion()
        {
            delistResourcesForTransaction();
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
        NeoStoreTransaction con = txConnectionMap.get( tx );
        if ( con != null )
        {
            try
            {
                con.delistResource(tx, XAResource.TMSUCCESS);
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
        NeoStoreTransaction con = txConnectionMap.remove( tx );
        if ( con != null )
        {
            con.destroy();
        }
    }

    public RelIdArray getCreatedNodes()
    {
        return getResource( true ).getCreatedNodes();
    }

    public boolean isNodeCreated( long nodeId )
    {
        return getResource( true ).isNodeCreated( nodeId );
    }

    public boolean isRelationshipCreated( long relId )
    {
        return getResource( true ).isRelationshipCreated( relId );
    }

    public int getKeyIdForProperty( PropertyData property )
    {
        return getReadOnlyResourceIfPossible().getKeyIdForProperty( property );
    }
}
