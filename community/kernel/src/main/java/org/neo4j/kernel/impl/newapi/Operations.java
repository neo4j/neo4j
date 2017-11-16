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

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.values.storable.Value;

/**
 * Collects all Kernel API operations and guards them from being used outside of transaction.
 */
public class Operations implements Read, ExplicitIndexRead, SchemaRead, Write
{
    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder allStoreHolder;
    private final StorageStatement statement;

    public Operations(
            StorageEngine engine,
            StorageStatement statement,
            KernelTransactionImplementation ktx,
            Supplier<ExplicitIndexTransactionState> explicitIndexes,
            Cursors cursors )
    {
        allStoreHolder = new AllStoreHolder( engine, statement, explicitIndexes, cursors );
        this.ktx = ktx;
        this.statement = statement;
    }

    // READ

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor,
            IndexOrder order, IndexQuery... query )
            throws KernelException
    {
        assertOpen();
        allStoreHolder.nodeIndexSeek( index, cursor, order, query );
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor, IndexOrder order ) throws KernelException
    {
        assertOpen();
        allStoreHolder.nodeIndexScan( index, cursor, order );
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        assertOpen();
        allStoreHolder.nodeLabelScan( label, cursor );
    }

    @Override
    public void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        assertOpen();
        allStoreHolder.nodeLabelUnionScan( cursor, labels );
    }

    @Override
    public void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        assertOpen();
        allStoreHolder.nodeLabelIntersectionScan( cursor, labels );
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        assertOpen();
        return allStoreHolder.nodeLabelScan( label );
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        assertOpen();
        allStoreHolder.allNodesScan( cursor );
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        assertOpen();
        return allStoreHolder.allNodesScan();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        assertOpen();
        allStoreHolder.singleNode( reference, cursor );
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        assertOpen();
        allStoreHolder.singleRelationship( reference, cursor );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        assertOpen();
        allStoreHolder.allRelationshipsScan( cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        assertOpen();
        return allStoreHolder.allRelationshipsScan();
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        assertOpen();
        allStoreHolder.relationshipLabelScan( label, cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        assertOpen();
        return allStoreHolder.relationshipLabelScan( label );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        assertOpen();
        allStoreHolder.relationshipGroups( nodeReference, reference, cursor );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        assertOpen();
        allStoreHolder.relationships( nodeReference, reference, cursor );
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        assertOpen();
        allStoreHolder.nodeProperties( reference, cursor );
    }

    @Override
    public void relationshipProperties( long reference, PropertyCursor cursor )
    {
        assertOpen();
        allStoreHolder.relationshipProperties( reference, cursor );
    }

    @Override
    public void graphProperties( PropertyCursor cursor )
    {
        assertOpen();
        allStoreHolder.graphProperties( cursor );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        assertOpen();
        allStoreHolder.futureNodeReferenceRead( reference );
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        assertOpen();
        allStoreHolder.futureRelationshipsReferenceRead( reference );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        assertOpen();
        allStoreHolder.futureNodePropertyReferenceRead( reference );
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        assertOpen();
        allStoreHolder.futureRelationshipPropertyReferenceRead( reference );
    }

    // EXPLICIT INDEX READ

    @Override
    public void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Value value )
            throws KernelException
    {
        assertOpen();
        allStoreHolder.nodeExplicitIndexLookup( cursor, index, key, value );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query )
            throws KernelException
    {
        assertOpen();
        allStoreHolder.nodeExplicitIndexQuery( cursor, index, query );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws KernelException
    {
        assertOpen();
        allStoreHolder.nodeExplicitIndexQuery( cursor, index, key, query );
    }

    @Override
    public void relationshipExplicitIndexGet( RelationshipExplicitIndexCursor cursor, String index, String key,
            Value value, long source, long target ) throws KernelException
    {
        assertOpen();
        allStoreHolder.relationshipExplicitIndexGet( cursor, index, key, value, source, target );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, Object query,
            long source, long target ) throws KernelException
    {
        assertOpen();
        allStoreHolder.relationshipExplicitIndexQuery( cursor, index, query, source, target );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, String key,
            Object query, long source, long target ) throws KernelException
    {
        assertOpen();
        allStoreHolder.relationshipExplicitIndexQuery( cursor, index, key, query, source, target );
    }

    // SCHEMA READ

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        assertOpen();
        return allStoreHolder.index( label, properties );
    }

    private void assertOpen()
    {
        if ( !ktx.isOpen() )
        {
            throw new NotInTransactionException( "The transaction has been closed." );
        }

        Optional<Status> terminationReason = ktx.getReasonIfTerminated();
        if ( terminationReason.isPresent() )
        {
            throw new TransactionTerminatedException( terminationReason.get() );
        }
    }

    // WRITE

    @Override
    public long nodeCreate()
    {
        assertOpen();
        long nodeId = statement.reserveNode();
        ktx.txState().nodeDoCreate( nodeId );
        return nodeId;
    }

    @Override
    public boolean nodeDelete( long node )
    {
        assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( ktx.txState().nodeIsAddedInThisTx( node ) )
            {
                ktx.txState().nodeDoDelete( node );
                return true;
            }
            if ( ktx.txState().nodeIsDeletedInThisTx( node ) )
            {
                // already deleted
                return false;
            }
        }

        if ( allStoreHolder.nodeExists( node ) )
        {
            ktx.txState().nodeDoDelete( node );
            return true;
        }

        // tried to delete node that does not exist
        return false;
    }

    @Override
    public long relationshipCreate( long sourceNode, int relationshipLabel, long targetNode )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipDelete( long relationship )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeAddLabel( long node, int nodeLabel ) throws KernelException
    {
        assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            boolean justAdded = ktx.txState().nodeIsAddedInThisTx( node );
            if ( justAdded && ktx.txState().nodeStateLabelDiffSets( node ).isAdded( nodeLabel ) )
            {
                //the newly added node already have the label
                return false;
            }
            else if ( justAdded )
            {
                // we have a new node, let's add the label
                ktx.txState().nodeDoAddLabel( nodeLabel, node );
                return true;
            }
            if ( ktx.txState().nodeIsDeletedInThisTx( node ) )
            {
                // already deleted, false or EntityNotFoundException?
                return false;
            }
        }

        //node was neither created nor deleted in this transaction
        //will throw if node isn't there
        if ( allStoreHolder.nodeHasLabel( node, nodeLabel ) )
        {
            return false;
        }
        //TODO indexTxStateUpdater.onLabelChange

        //node is there and doesn't already have the label
        //let's add it
        ktx.txState().nodeDoAddLabel( nodeLabel, node );
        return true;
    }

    @Override
    public boolean nodeRemoveLabel( long node, int nodeLabel ) throws KernelException
    {
        assertOpen();
        //TODO indexTxStateUpdater.onLabelChange
        if ( !allStoreHolder.nodeHasLabel( node, nodeLabel ) )
        {
            return false;
        }

        ktx.txState().nodeDoRemoveLabel( nodeLabel, node );
        return true;
    }

    @Override
    public Value nodeSetProperty( long node, int propertyKey, Value value )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value nodeRemoveProperty( long node, int propertyKey )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value relationshipSetProperty( long relationship, int propertyKey, Value value )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value relationshipRemoveProperty( long node, int propertyKey )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value graphSetProperty( int propertyKey, Value value )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }

    @Override
    public Value graphRemoveProperty( int propertyKey )
    {
        assertOpen();
        throw new UnsupportedOperationException();
    }
}
