/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.Map;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Collects all Kernel API operations and guards them from being used outside of transaction.
 */
public class Operations implements Write, ExplicitIndexWrite
{
    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder allStoreHolder;
    private final StorageStatement statement;
    private final AutoIndexing autoIndexing;
    private org.neo4j.kernel.impl.newapi.NodeCursor nodeCursor;
    private final IndexTxStateUpdater updater;
    private PropertyCursor propertyCursor;
    private final Cursors cursors;

    public Operations(
            AllStoreHolder allStoreHolder,
            IndexTxStateUpdater updater,
            StorageStatement statement,
            KernelTransactionImplementation ktx,
            Cursors cursors, AutoIndexing autoIndexing  )
    {
        this.autoIndexing = autoIndexing;
        this.allStoreHolder = allStoreHolder;
        this.ktx = ktx;
        this.statement = statement;
        this.updater = updater;
        this.cursors = cursors;
    }

    public void initialize()
    {
        this.nodeCursor = cursors.allocateNodeCursor();
        this.propertyCursor = cursors.allocatePropertyCursor();
    }

    @Override
    public long nodeCreate()
    {
        ktx.assertOpen();
        long nodeId = statement.reserveNode();
        ktx.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public boolean nodeDelete( long node ) throws AutoIndexingKernelException
    {
        ktx.assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( ktx.txState().nodeIsAddedInThisTx( node ) )
            {
                autoIndexing.nodes().entityRemoved( this, node );
                ktx.txState().nodeDoDelete( node );
                return true;
            }
            if ( ktx.txState().nodeIsDeletedInThisTx( node ) )
            {
                // already deleted
                return false;
            }
        }

        ktx.locks().optimistic().acquireExclusive( ktx.lockTracer(), ResourceTypes.NODE, node );
        if ( allStoreHolder.nodeExistsInStore( node ) )
        {
            autoIndexing.nodes().entityRemoved( this, node );
            ktx.txState().nodeDoDelete( node );
            return true;
        }

        // tried to delete node that does not exist
        return false;
    }

    @Override
    public long relationshipCreate( long sourceNode, int relationshipLabel, long targetNode )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipDelete( long relationship )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeAddLabel( long node, int nodeLabel ) throws EntityNotFoundException
    {
        acquireSharedLabelLock( nodeLabel );
        acquireExclusiveNodeLock( node );

        ktx.assertOpen();
        allStoreHolder.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, node );
        }

        if ( nodeCursor.labels().contains( nodeLabel ) )
        {
            //label already there, nothing to do
            return false;
        }

        //node is there and doesn't already have the label, let's add
        ktx.txState().nodeDoAddLabel( nodeLabel, node );
        updater.onLabelChange( nodeLabel, nodeCursor, propertyCursor, ADDED_LABEL );
        return true;
    }

    @Override
    public boolean nodeRemoveLabel( long node, int nodeLabel ) throws EntityNotFoundException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();

        allStoreHolder.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, node );
        }

        if ( !nodeCursor.labels().contains( nodeLabel ) )
        {
            //the label wasn't there, nothing to do
            return false;
        }

        ktx.txState().nodeDoRemoveLabel( nodeLabel, node );
        updater.onLabelChange( nodeLabel, nodeCursor, propertyCursor, REMOVED_LABEL );
        return true;
    }

    @Override
    public Value nodeSetProperty( long node, int propertyKey, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();

        Value existingValue = readNodeProperty( node, propertyKey );

        if ( existingValue == NO_VALUE )
        {
            //no existing value, we just add it
            autoIndexing.nodes().propertyAdded( this, node, propertyKey, value );
            ktx.txState().nodeDoAddProperty( node, propertyKey, value );
            updater.onPropertyAdd( nodeCursor, propertyCursor, propertyKey, value );
            return NO_VALUE;
        }
        else
        {
            if ( !value.equals( existingValue ) )
            {
                //the value has changed to a new value
                autoIndexing.nodes().propertyChanged( this, node, propertyKey, existingValue, value );
                ktx.txState().nodeDoChangeProperty( node, propertyKey, existingValue, value );
                updater.onPropertyChange( nodeCursor, propertyCursor, propertyKey, existingValue, value );
            }
            return existingValue;
        }
    }

    @Override
    public Value nodeRemoveProperty( long node, int propertyKey )
            throws EntityNotFoundException, AutoIndexingKernelException
    {
        acquireExclusiveNodeLock( node );
        ktx.assertOpen();
        Value existingValue = readNodeProperty( node, propertyKey );

        if ( existingValue != NO_VALUE )
        {
            //no existing value, we just add it
            autoIndexing.nodes().propertyRemoved( this, node, propertyKey);
            ktx.txState().nodeDoRemoveProperty( node, propertyKey, existingValue);
            updater.onPropertyRemove( nodeCursor, propertyCursor, propertyKey, existingValue );
        }

        return existingValue;
    }

    @Override
    public Value relationshipSetProperty( long relationship, int propertyKey, Value value )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value relationshipRemoveProperty( long node, int propertyKey )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value graphSetProperty( int propertyKey, Value value )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value graphRemoveProperty( int propertyKey )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeAddToExplicitIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().nodeChanges( indexName ).addNode( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().nodeChanges( indexName ).remove( node );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().nodeChanges( indexName ).remove( node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().nodeChanges( indexName ).remove( node, key );
    }

    @Override
    public void nodeExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().createIndex( IndexEntityType.Node, indexName, customConfig );
    }

    @Override
    public void nodeExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    @Override
    public void relationshipAddToExplicitIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        ktx.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship, key );

    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        ktx.explicitIndexTxState().relationshipChanges( indexName ).remove( relationship );
    }

    @Override
    public void relationshipExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        ktx.explicitIndexTxState().createIndex( IndexEntityType.Relationship, indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        ktx.assertOpen();
        allStoreHolder.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    private Value readNodeProperty( long node, int propertyKey ) throws EntityNotFoundException
    {
        allStoreHolder.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, node );
        }

        nodeCursor.properties( propertyCursor );

        //Find out if the property had a value
        Value existingValue = NO_VALUE;
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKey )
            {
                existingValue = propertyCursor.propertyValue();
                break;
            }
        }
        return existingValue;
    }

    public CursorFactory cursors()
    {
        return cursors;
    }

    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.close();
            nodeCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.close();
            propertyCursor = null;
        }
    }

    public Token token()
    {
        return allStoreHolder;
    }

    private void acquireExclusiveNodeLock( long node )
    {
        if ( !ktx.hasTxStateWithChanges() || !ktx.txState().nodeIsAddedInThisTx( node ) )
        {
            ktx.locks().optimistic().acquireExclusive( ktx.lockTracer(), ResourceTypes.NODE, node );
        }
    }

    private void acquireSharedLabelLock( int labelId )
    {
        ktx.locks().optimistic().acquireShared( ktx.lockTracer(), ResourceTypes.LABEL, labelId );
    }

    public ExplicitIndexRead indexRead()
    {
        return allStoreHolder;
    }

    public SchemaRead schemaRead()
    {
        return allStoreHolder;
    }

    public Read dataRead()
    {
        return allStoreHolder;
    }

    public NodeCursor nodeCursor()
    {
        return nodeCursor;
    }

    public PropertyCursor propertyCursor()
    {
        return propertyCursor;
    }
}
