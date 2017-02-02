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
package org.neo4j.kernel.impl.api.cursor;

import java.util.function.Consumer;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.EntityItemHelper;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.DegreeItem;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.txstate.NodeState;

import static org.neo4j.collection.primitive.Primitive.intSet;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.filter;
import static org.neo4j.kernel.impl.util.Cursors.empty;

/**
 * Overlays transaction state on a {@link NodeItem} cursor.
 */
public class TxSingleNodeCursor extends EntityItemHelper implements Cursor<NodeItem>, NodeItem
{
    private final TransactionState state;
    private final Consumer<TxSingleNodeCursor> cache;

    private long id = StatementConstants.NO_SUCH_NODE;
    private Cursor<NodeItem> cursor;
    private NodeState nodeState;
    private boolean nodeIsAddedInThisTx;

    public TxSingleNodeCursor( TransactionState state, Consumer<TxSingleNodeCursor> cache )
    {
        this.state = state;
        this.cache = cache;
    }

    public TxSingleNodeCursor init( Cursor<NodeItem> nodeCursor, long nodeId )
    {
        this.id = nodeId;
        this.cursor = nodeCursor;
        this.nodeIsAddedInThisTx = state.nodeIsAddedInThisTx( id );
        return this;
    }

    @Override
    public NodeItem get()
    {
        if ( id == StatementConstants.NO_SUCH_NODE )
        {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public boolean next()
    {
        if ( id == StatementConstants.NO_SUCH_NODE )
        {
            return false;
        }

        if ( state.nodeIsDeletedInThisTx( id ) )
        {
            this.id = StatementConstants.NO_SUCH_NODE;
            return false;
        }

        if ( cursor.next() || nodeIsAddedInThisTx )
        {
            // this makes sure we read the node from tx state only once and we do not loop forever
            nodeIsAddedInThisTx = false;
            this.nodeState = state.getNodeState( id );
            return true;
        }
        else
        {
            this.id = StatementConstants.NO_SUCH_NODE;
            this.nodeState = null;
            return false;
        }
    }

    @Override
    public void close()
    {
        cursor.close();
        cursor = null;
        cache.accept( this );
    }

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public PrimitiveIntSet labels()
    {
        return state.augmentLabels( nodeIsAddedInThisTx ? intSet() : this.cursor.get().labels(), nodeState );
    }

    @Override
    public boolean hasLabel( int labelId )
    {
        if ( nodeIsAddedInThisTx || nodeState.labelDiffSets().getRemoved().contains( labelId ) )
        {
            return false;
        }
        if ( nodeState.labelDiffSets().getAdded().contains( labelId ) )
        {
            return true;
        }
        return this.cursor.get().hasLabel( labelId );
    }

    @Override
    public Cursor<PropertyItem> properties()
    {
        return state.augmentPropertyCursor( nodeIsAddedInThisTx ? empty() : cursor.get().properties(), nodeState );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        Cursor<PropertyItem> cursor = nodeIsAddedInThisTx ? empty() : this.cursor.get().property( propertyKeyId );
        return state.augmentSinglePropertyCursor( cursor, nodeState, propertyKeyId );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
    {
        Cursor<RelationshipItem> cursor =
                nodeIsAddedInThisTx ? empty() : this.cursor.get().relationships( direction, relTypes );
        return state.augmentNodeRelationshipCursor( cursor, nodeState, direction, relTypes );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction )
    {
        Cursor<RelationshipItem> cursor = nodeIsAddedInThisTx ? empty() : this.cursor.get().relationships( direction );
        return state.augmentNodeRelationshipCursor( cursor, nodeState, direction, null );
    }

    @Override
    public PrimitiveIntSet relationshipTypes()
    {
        if ( nodeIsAddedInThisTx )
        {
            return nodeState.relationshipTypes();
        }

        // Read types in the current transaction
        PrimitiveIntSet types =  nodeState.relationshipTypes();

        // Augment with types stored on disk, minus any types where all rels of that type are deleted
        // in current tx.
        types.addAll( filter( cursor.get().relationshipTypes().iterator(),
                ( current ) -> !types.contains( current ) && degree( Direction.BOTH, current ) > 0 ) );

        return types;
    }

    @Override
    public int degree( Direction direction )
    {
        return nodeState.augmentDegree( direction, nodeIsAddedInThisTx ? 0 : cursor.get().degree( direction ) );
    }

    @Override
    public int degree( Direction direction, int relType )
    {
        int degree = nodeIsAddedInThisTx ? 0 : cursor.get().degree( direction, relType );
        return nodeState.augmentDegree( direction, degree, relType );
    }

    @Override
    public Cursor<DegreeItem> degrees()
    {
        return new DegreeCursor( relationshipTypes().iterator() );
    }

    @Override
    public boolean isDense()
    {
        return cursor.get().isDense();
    }

    private class DegreeCursor implements Cursor<DegreeItem>, DegreeItem
    {
        private final PrimitiveIntIterator relTypeCursor;
        private int type;
        private long outgoing;
        private long incoming;

        DegreeCursor( PrimitiveIntIterator relTypeCursor )
        {
            this.relTypeCursor = relTypeCursor;
        }

        @Override
        public boolean next()
        {
            boolean hasNext = relTypeCursor.hasNext();
            if ( hasNext )
            {
                type = relTypeCursor.next();
                outgoing = degree( Direction.OUTGOING, type );
                incoming = degree( Direction.INCOMING, type );
            }
            return hasNext;
        }

        @Override
        public void close()
        {
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public long outgoing()
        {
            return outgoing;
        }

        @Override
        public long incoming()
        {
            return incoming;
        }

        @Override
        public DegreeItem get()
        {
            return this;
        }
    }
}
