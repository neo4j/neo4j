/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.GenericCursor;
import org.neo4j.cursor.IntValue;
import org.neo4j.function.Consumer;
import org.neo4j.function.IntSupplier;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.DegreeItem;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.util.Cursors;

/**
 * Overlays transaction state on a {@link NodeItem} cursor.
 */
public abstract class TxAbstractNodeCursor
        extends NodeItem.NodeItemHelper
        implements Cursor<NodeItem>, NodeItem
{
    protected final TransactionState state;
    private final Consumer<TxAbstractNodeCursor> cache;

    protected Cursor<NodeItem> cursor;

    protected long id;

    protected NodeState nodeState;
    protected boolean nodeIsAddedInThisTx;

    public TxAbstractNodeCursor( TransactionState state, Consumer<TxAbstractNodeCursor> cache )
    {
        this.state = state;
        this.cache = cache;
    }

    public TxAbstractNodeCursor init( Cursor<NodeItem> nodeCursor )
    {
        this.cursor = nodeCursor;
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
    public Cursor<LabelItem> labels()
    {
        return state.augmentLabelCursor( nodeIsAddedInThisTx ? Cursors.<LabelItem>empty() : cursor.get().labels(),
                nodeState );
    }

    @Override
    public Cursor<LabelItem> label( int labelId )
    {
        return state.augmentSingleLabelCursor(
                nodeIsAddedInThisTx ? Cursors.<LabelItem>empty() : cursor.get().label( labelId ),
                nodeState, labelId );
    }

    @Override
    public Cursor<PropertyItem> properties()
    {
        return state.augmentPropertyCursor(
                nodeIsAddedInThisTx ? Cursors.<PropertyItem>empty() : cursor.get().properties(),
                nodeState );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        return state.augmentSinglePropertyCursor(
                nodeIsAddedInThisTx ? Cursors.<PropertyItem>empty() : cursor.get().property( propertyKeyId ),
                nodeState, propertyKeyId );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
    {
        return state.augmentNodeRelationshipCursor(
                nodeIsAddedInThisTx ? Cursors.<RelationshipItem>empty() : cursor.get().relationships( direction,
                        relTypes ), nodeState,
                direction, relTypes );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction )
    {
        return state.augmentNodeRelationshipCursor(
                nodeIsAddedInThisTx ? Cursors.<RelationshipItem>empty() : cursor.get().relationships( direction ),
                nodeState,
                direction, null );
    }

    @Override
    public Cursor<IntSupplier> relationshipTypes()
    {
        if ( nodeIsAddedInThisTx )
        {
            return new RelationshipTypeCursor( nodeState.relationshipTypes() );
        }

        PrimitiveIntSet types = Primitive.intSet();

        // Add types in the current transaction
        PrimitiveIntIterator typesInTx = nodeState.relationshipTypes();
        while ( typesInTx.hasNext() )
        {
            types.add( typesInTx.next() );
        }

        // Augment with types stored on disk, minus any types where all rels of that type are deleted
        // in current tx.
        try ( Cursor<IntSupplier> storeTypes = cursor.get().relationshipTypes() )
        {
            while ( storeTypes.next() )
            {
                int current = storeTypes.get().getAsInt();
                if ( !types.contains( current ) && degree( Direction.BOTH, current ) > 0 )
                {
                    types.add( current );
                }
            }
        }
        return new RelationshipTypeCursor( types.iterator() );
    }

    @Override
    public int degree( Direction direction )
    {
        return nodeState.augmentDegree( direction, nodeIsAddedInThisTx ? 0 : cursor.get().degree( direction ) );
    }

    @Override
    public int degree( Direction direction, int relType )
    {
        return nodeState.augmentDegree( direction, nodeIsAddedInThisTx ? 0 : cursor.get().degree( direction, relType ),
                relType );
    }

    @Override
    public Cursor<DegreeItem> degrees()
    {
        return new DegreeCursor( relationshipTypes() );
    }

    @Override
    public boolean isDense()
    {
        return cursor.get().isDense();
    }

    private class RelationshipTypeCursor extends GenericCursor<IntSupplier>
    {
        private PrimitiveIntIterator primitiveIntIterator;

        public RelationshipTypeCursor( PrimitiveIntIterator primitiveIntIterator )
        {
            this.primitiveIntIterator = primitiveIntIterator;
            current = new IntValue();
        }

        @Override
        public boolean next()
        {
            if ( primitiveIntIterator.hasNext() )
            {
                ((IntValue) current).setValue( primitiveIntIterator.next() );
                return true;
            }
            else
            {
                current = null;
                return false;
            }
        }
    }

    private class DegreeCursor implements Cursor<DegreeItem>, DegreeItem
    {
        private Cursor<IntSupplier> relTypeCursor;
        private int type;
        private long outgoing;
        private long incoming;

        public DegreeCursor( Cursor<IntSupplier> relTypeCursor )
        {
            this.relTypeCursor = relTypeCursor;
        }

        @Override
        public boolean next()
        {
            if ( relTypeCursor.next() )
            {
                type = relTypeCursor.get().getAsInt();
                outgoing = degree( Direction.OUTGOING, type );
                incoming = degree( Direction.INCOMING, type );

                return true;
            }
            else
            {
                return false;
            }
        }

        @Override
        public void close()
        {
            relTypeCursor.close();
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
