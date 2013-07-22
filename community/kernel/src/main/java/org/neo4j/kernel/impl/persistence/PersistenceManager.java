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
package org.neo4j.kernel.impl.persistence;

import java.util.Map;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.StringLogger;

public class PersistenceManager
{
    private final PersistenceSource persistenceSource;
    private final StringLogger msgLog;
    private final AbstractTransactionManager transactionManager;

    private final ArrayMap<Transaction,NeoStoreTransaction> txConnectionMap = new ArrayMap<>( (byte) 5, true, true );

    private final TxEventSyncHookFactory syncHookFactory;

    public PersistenceManager( StringLogger msgLog, AbstractTransactionManager transactionManager,
            PersistenceSource persistenceSource,
            TxEventSyncHookFactory syncHookFactory )
    {
        this.msgLog = msgLog;
        this.transactionManager = transactionManager;
        this.persistenceSource = persistenceSource;
        this.syncHookFactory = syncHookFactory;
    }

    public NodeRecord loadLightNode( long id )
    {
        return getReadOnlyResourceIfPossible().nodeLoadLight( id );
    }

    public Object nodeLoadPropertyValue( long nodeId, int propertyKey )
    {
        return getReadOnlyResource().nodeLoadPropertyValue( nodeId, propertyKey );
    }
    
    public Object relationshipLoadPropertyValue( long relationshipId, int propertyKey )
    {
        return getReadOnlyResource().relationshipLoadPropertyValue( relationshipId, propertyKey );
    }
    
    public Object graphLoadPropertyValue( int propertyKey )
    {
        return getReadOnlyResource().graphLoadPropertyValue( propertyKey );
    }

    public Token[] loadAllPropertyKeyTokens()
    {
        /**
         * Using getReadOnlyResource here since {@link #loadAllPropertyKeyTokens()} doesn't
         * follow transaction state visibility standard anyway. There's also
         * an issue where this is called right after the neostore data source
         * has been started, but potentially before all recovery has been
         * done on all data sources, meaning that a call to getTransaction
         * could fail.
         */
        return getReadOnlyResource/*IfPossible*/().loadAllPropertyKeyTokens();
    }

    public Token[] loadAllLabelTokens()
    {
        /**
         * Using getReadOnlyResource here since {@link #loadAllLabelTokens()} doesn't
         * follow transaction state visibility standard anyway. There's also
         * an issue where this is called right after the neostore data source
         * has been started, but potentially before all recovery has been
         * done on all data sources, meaning that a call to getTransaction
         * could fail.
         */
        return getReadOnlyResource/*IfPossible*/().loadAllLabelTokens();
    }

    public Token[] loadAllRelationshipTypeTokens()
    {
        /**
         * Using getReadOnlyResource here since {@link #loadAllRelationshipTypeTokens()} doesn't
         * follow transaction state visibility standard anyway. There's also
         * an issue where this is called right after the neostore data source
         * has been started, but potentially before all recovery has been
         * done on all data sources, meaning that a call to getTransaction
         * could fail.
         */
        return getReadOnlyResource/*IfPossible*/().loadRelationshipTypes();
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

    public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
    {
        return getResource( true ).nodeDelete( nodeId );
    }

    public PropertyData nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource( true ).nodeAddProperty( nodeId, propertyKey, value );
    }

    public PropertyData nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource( true ).nodeChangeProperty( nodeId, propertyKey, value );
    }

    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        getResource( true ).nodeRemoveProperty( nodeId, propertyKey );
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

    public PropertyData relAddProperty( long relId, int propertyKey, Object value )
    {
        return getResource( true ).relAddProperty( relId, propertyKey, value );
    }

    public PropertyData relChangeProperty( long relId, int propertyKey, Object value )
    {
        return getResource( true ).relChangeProperty( relId, propertyKey, value );
    }

    public void relRemoveProperty( long relId, int propertyKey )
    {
        getResource( true ).relRemoveProperty( relId, propertyKey );
    }

    public PropertyData graphAddProperty( int propertyKey, Object value )
    {
        return getResource( true ).graphAddProperty( propertyKey, value );
    }

    public PropertyData graphChangeProperty( int propertyKey, Object value )
    {
        return getResource( true ).graphChangeProperty( propertyKey, value );
    }

    public void graphRemoveProperty( int propertyKey )
    {
        getResource( true ).graphRemoveProperty( propertyKey );
    }
    
    public ArrayMap<Integer, PropertyData> graphLoadProperties( boolean light )
    {
        return getReadOnlyResourceIfPossible().graphLoadProperties( light );
    }
    
    public void createPropertyKeyToken( String key, int id )
    {
        getResource( true ).createPropertyKeyToken( key, id );
    }

    public void createLabelId( String name, int id )
    {
        getResource( true ).createLabelToken( name, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource( false ).createRelationshipTypeToken( id, name );
    }

    private NeoStoreTransaction getReadOnlyResource()
    {
        return ((NioNeoDbPersistenceSource)
                persistenceSource ).createReadOnlyResourceConnection();
    }

    private NeoStoreTransaction getReadOnlyResourceIfPossible()
    {
        Transaction tx = this.getCurrentTransaction();

        NeoStoreTransaction con = txConnectionMap.get( tx );
        if ( con == null )
        {
            return getReadOnlyResource();
        }
        return con;
    }

    private NeoStoreTransaction getResource( boolean registerEventHooks )
    {
        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        NeoStoreTransaction con = txConnectionMap.get( tx );
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
            catch ( RollbackException re )
            {
                String msg = "The transaction is marked for rollback only.";
                throw new ResourceAcquisitionFailedException( msg, re );
            }
            catch ( SystemException se )
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

    public void dropSchemaRule( long ruleId )
    {
        getResource( true ).dropSchemaRule( ruleId );
    }

    public void setConstraintIndexOwner( long constraintIndexId, long constraintId )
    {
        getResource( true ).setConstraintIndexOwner( constraintIndexId, constraintId );
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
                msgLog.error( "Error releasing resources for " + tx, t );
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

    public void createSchemaRule( SchemaRule rule )
    {
        getResource( true ).createSchemaRule( rule );
    }

    public void addLabelToNode( long labelId, long nodeId )
    {
        getResource( true ).addLabelToNode( labelId, nodeId );
    }
    
    public void removeLabelFromNode( long labelId, long nodeId )
    {
        getResource( true ).removeLabelFromNode( labelId, nodeId );
    }

    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        return getReadOnlyResourceIfPossible().getLabelsForNode( nodeId );
    }
}
