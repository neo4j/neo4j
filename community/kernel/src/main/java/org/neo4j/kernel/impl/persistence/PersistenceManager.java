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
import org.neo4j.kernel.api.exceptions.ReleaseLocksFailedKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction.PropertyReceiver;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
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
        return getResource().forReading().nodeLoadLight( id );
    }

    public long getRelationshipChainPosition( long nodeId )
    {
        return getResource().forReading().getRelationshipChainPosition( nodeId );
    }

    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position )
    {
        return getResource().forReading().getMoreRelationships( nodeId, position );
    }

    public void loadNodeProperties( long nodeId, boolean light, PropertyReceiver receiver )
    {
        getResource().forReading().nodeLoadProperties( nodeId, light, receiver );
    }

    public void loadRelProperties( long relId, boolean light, PropertyReceiver receiver )
    {
        getResource().forReading().relLoadProperties( relId, light, receiver );
    }

    public RelationshipRecord loadLightRelationship( long id )
    {
        return getResource().forReading().relLoadLight( id );
    }

    public ArrayMap<Integer,DefinedProperty> nodeDelete( long nodeId )
    {
        return getResource().forWriting().nodeDelete( nodeId );
    }

    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource().forWriting().nodeAddProperty( nodeId, propertyKey, value );
    }

    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        return getResource().forWriting().nodeChangeProperty( nodeId, propertyKey, value );
    }

    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        getResource().forWriting().nodeRemoveProperty( nodeId, propertyKey );
    }

    public void nodeCreate( long id )
    {
        getResource().forWriting().nodeCreate( id );
    }

    public void relationshipCreate( long id, int typeId, long startNodeId,
        long endNodeId )
    {
        getResource().forWriting().relationshipCreate( id, typeId, startNodeId, endNodeId );
    }

    public ArrayMap<Integer,DefinedProperty> relDelete( long relId )
    {
        return getResource().forWriting().relDelete( relId );
    }

    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        return getResource().forWriting().relAddProperty( relId, propertyKey, value );
    }

    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        return getResource().forWriting().relChangeProperty( relId, propertyKey, value );
    }

    public void relRemoveProperty( long relId, int propertyKey )
    {
        getResource().forWriting().relRemoveProperty( relId, propertyKey );
    }

    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        return getResource().forWriting().graphAddProperty( propertyKey, value );
    }

    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        return getResource().forWriting().graphChangeProperty( propertyKey, value );
    }

    public void graphRemoveProperty( int propertyKey )
    {
        getResource().forWriting().graphRemoveProperty( propertyKey );
    }

    public void graphLoadProperties( boolean light, PropertyReceiver receiver )
    {
        getResource().forReading().graphLoadProperties( light, receiver );
    }

    public void createPropertyKeyToken( String key, int id )
    {
        getResource().forWriting().createPropertyKeyToken( key, id );
    }

    public void createLabelId( String name, int id )
    {
        getResource().forWriting().createLabelToken( name, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource().forWriting().createRelationshipTypeToken( id, name );
    }

    public void dropSchemaRule( SchemaRule rule )
    {
        getResource().forWriting().dropSchemaRule( rule );
    }

    public void setConstraintIndexOwner( IndexRule constraintIndex, long constraintId )
    {
        getResource().forWriting().setConstraintIndexOwner( constraintIndex, constraintId );
    }

    public void createSchemaRule( SchemaRule rule )
    {
        getResource().forWriting().createSchemaRule( rule );
    }

    public void addLabelToNode( int labelId, long nodeId )
    {
        getResource().forWriting().addLabelToNode( labelId, nodeId );
    }

    public void removeLabelFromNode( int labelId, long nodeId )
    {
        getResource().forWriting().removeLabelFromNode( labelId, nodeId );
    }

    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        return getResource().forReading().getLabelsForNode( nodeId );
    }

    public KernelTransaction currentKernelTransactionForReading()
    {
        return getResource().forReading().kernelTransaction();
    }

    public KernelTransaction currentKernelTransactionForWriting()
    {
        return getResource().forWriting().kernelTransaction();
    }

    public void ensureKernelIsEnlisted()
    {
        getResource();
    }

    public ResourceHolder getResource()
    {
        TransactionState txState = transactionManager.getTransactionState();
        ResourceHolder resource = txState.getNeoStoreTransaction();
        if ( resource == null )
        {
            txState.setNeoStoreTransaction( resource = createResource( getCurrentTransaction() ) );
        }
        return resource;
    }

    private ResourceHolder createResource( Transaction tx )
    {
        try
        {
            XaConnection xaConnection = persistenceSource.getXaDataSource().getXaConnection();
            NeoStoreTransaction resource = persistenceSource.createTransaction( xaConnection );
            ResourceHolder result = new ResourceHolder( syncHookFactory, tx, xaConnection, resource );

            TransactionState state = transactionManager.getTransactionState();
            tx.registerSynchronization( new ResourceCleanupHook( tx, state, result ) );
            return result;
        }
        catch ( RollbackException e )
        {
            throw new ResourceAcquisitionFailedException( e );
        }
        catch ( SystemException e )
        {
            throw new ResourceAcquisitionFailedException( e );
        }
    }

    public Transaction getCurrentTransaction()
        throws NotInTransactionException
    {
        try
        {
            Transaction tx = transactionManager.getTransaction();
            if ( tx == null )
            {
                throw new NotInTransactionException();
            }
            return tx;
        }
        catch ( SystemException se )
        {
            throw new TransactionFailureException( "Error fetching transaction "
                + "for current thread", se );
        }
    }

    private class ResourceCleanupHook implements Synchronization
    {
        private final Transaction tx;
        private final TransactionState state;
        private final ResourceHolder resourceHolder;

        ResourceCleanupHook( Transaction tx, TransactionState state, ResourceHolder resourceHolder )
        {
            this.tx = tx;
            this.state = state;
            this.resourceHolder = resourceHolder;
        }

        @Override
        public void afterCompletion( int param )
        {
            try
            {
                releaseConnections( tx );
                // Release locks held in the old transaction state
                if ( param == Status.STATUS_COMMITTED )
                {
                    state.commit();
                }
                else
                {
                    state.rollback();
                }
            }
            finally
            {
                // Release locks held by the kernel API stack
                try
                {
                    resourceHolder.resource.kernelTransaction().release();
                }
                catch ( ReleaseLocksFailedKernelException e )
                {
                    msgLog.error( "Error releasing resources for " + tx, e );
                }
            }
        }

        @Override
        public void beforeCompletion()
        {
            resourceHolder.delist();
        }

        private void releaseConnections( Transaction tx )
        {
            try
            {
                releaseResourceConnectionsForTransaction( tx, state );
            }
            catch ( Throwable t )
            {
                msgLog.error( "Error releasing resources for " + tx, t );
            }
        }
    }

    void releaseResourceConnectionsForTransaction( Transaction tx, TransactionState state )
        throws NotInTransactionException
    {
        ResourceHolder resource = state.getNeoStoreTransaction();
        if ( resource != null )
        {
            resource.destroy();
        }
    }

    public static class ResourceHolder
    {
        private final TxEventSyncHookFactory syncHookFactory;
        private final Transaction tx;
        private final XaConnection connection;
        private final NeoStoreTransaction resource;
        private boolean enlisted;

        ResourceHolder( TxEventSyncHookFactory syncHookFactory,
                Transaction tx, XaConnection connection, NeoStoreTransaction resource )
        {
            this.syncHookFactory = syncHookFactory;
            this.tx = tx;
            this.connection = connection;
            this.resource = resource;
        }

        public NeoStoreTransaction forReading()
        {
            return resource;
        }

        public NeoStoreTransaction forWriting()
        {
            if ( !enlisted )
            {
                enlist();
                enlisted = true;
            }
            return resource;
        }

        private void enlist()
        {
            try
            {
                XAResource xaResource = connection.getXaResource();
                if ( !tx.enlistResource( xaResource ) )
                {
                    throw new ResourceAcquisitionFailedException( xaResource );
                }

                TransactionEventsSyncHook hook = syncHookFactory.create();
                if ( hook != null )
                {
                    tx.registerSynchronization( hook );
                }
            }
            catch ( RollbackException re )
            {
                throw new ResourceAcquisitionFailedException( re );
            }
            catch ( SystemException se )
            {
                throw new ResourceAcquisitionFailedException( se );
            }
        }

        public void delist()
        {
            if ( enlisted )
            {
                try
                {
                    connection.delistResource( tx, XAResource.TMSUCCESS );
                }
                catch ( SystemException e )
                {
                    throw new TransactionFailureException(
                            "Failed to delist resource '" + resource + "' from current transaction.", e );
                }
            }
        }

        void destroy()
        {
            connection.destroy();
        }
    }
}
