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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.persistence.NeoStoreTransaction.PropertyReceiver;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * The persistence manager contains all data and schema operations that Neo4j can perform, and is essentially only
 * a bridge that ensures there is a WriteTransaction or ReadTransaction running, to which it delegates all calls.
 *
 * This should go away, and be replaced by a mechanism that returns a KernelTransaction that is ensured to be registered
 * with the transaction manager.
 *
 * Ironically, once we finish up the move of operations into the new Kernel, the methods in here should only ever get
 * called by KernelTransaction and its sub components, so removing those operations should be a simple matter of moving
 * the implementation into the Kernel.
 */
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

    public KernelTransaction currentKernelTransaction()
    {
        return getResource().kernelTransaction();
    }

    public void ensureKernelIsEnlisted()
    {
        getResource();
    }

    public NodeRecord loadLightNode( long id )
    {
        return getResource().nodeLoadLight( id );
    }

    public long getRelationshipChainPosition( long nodeId )
    {
        return getResource().getRelationshipChainPosition( nodeId );
    }

    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position )
    {
        return getResource().getMoreRelationships( nodeId, position );
    }

    public void loadNodeProperties( long nodeId, boolean light, PropertyReceiver receiver )
    {
        getResource().nodeLoadProperties( nodeId, light, receiver );
    }

    public void loadRelProperties( long relId, boolean light, PropertyReceiver receiver )
    {
        getResource().relLoadProperties( relId, light, receiver );
    }

    public RelationshipRecord loadLightRelationship( long id )
    {
        return getResource().relLoadLight( id );
    }

    public ArrayMap<Integer,DefinedProperty> nodeDelete( long nodeId )
    {
        return getResource().nodeDelete( nodeId );
    }

    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource().nodeAddProperty( nodeId, propertyKey, value );
    }

    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource().nodeChangeProperty( nodeId, propertyKey, value );
    }

    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        getResource().nodeRemoveProperty( nodeId, propertyKey );
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

    public ArrayMap<Integer,DefinedProperty> relDelete( long relId )
    {
        return getResource().relDelete( relId );
    }

    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        return getResource().relAddProperty( relId, propertyKey, value );
    }

    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        return getResource().relChangeProperty( relId, propertyKey, value );
    }

    public void relRemoveProperty( long relId, int propertyKey )
    {
        getResource().relRemoveProperty( relId, propertyKey );
    }

    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        return getResource().graphAddProperty( propertyKey, value );
    }

    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        return getResource().graphChangeProperty( propertyKey, value );
    }

    public void graphRemoveProperty( int propertyKey )
    {
        getResource().graphRemoveProperty( propertyKey );
    }

    public void graphLoadProperties( boolean light, PropertyReceiver receiver )
    {
        getResource().graphLoadProperties( light, receiver );
    }

    public void createPropertyKeyToken( String key, int id )
    {
        getResource().createPropertyKeyToken( key, id );
    }

    public void createLabelId( String name, int id )
    {
        getResource().createLabelToken( name, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource().createRelationshipTypeToken( id, name );
    }

    private NeoStoreTransaction getResource()
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

    public void dropSchemaRule( SchemaRule rule )
    {
        getResource().dropSchemaRule( rule );
    }

    public void setConstraintIndexOwner( IndexRule constraintIndex, long constraintId )
    {
        getResource().setConstraintIndexOwner( constraintIndex, constraintId );
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
        getResource().createSchemaRule( rule );
    }

    public void addLabelToNode( int labelId, long nodeId )
    {
        getResource().addLabelToNode( labelId, nodeId );
    }

    public void removeLabelFromNode( int labelId, long nodeId )
    {
        getResource().removeLabelFromNode( labelId, nodeId );
    }

    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        return getResource().getLabelsForNode( nodeId );
    }
}
