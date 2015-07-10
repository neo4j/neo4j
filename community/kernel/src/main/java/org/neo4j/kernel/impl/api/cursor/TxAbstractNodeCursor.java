/*
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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.StatementConstants;
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
}
