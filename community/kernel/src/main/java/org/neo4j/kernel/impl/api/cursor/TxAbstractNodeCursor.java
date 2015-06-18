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

import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.state.NodeState;

/**
 * Overlays transaction state on a {@link NodeCursor}.
 */
public abstract class TxAbstractNodeCursor
        implements NodeCursor
{
    protected final TransactionState state;
    private final Consumer<TxAbstractNodeCursor> cache;

    protected NodeCursor cursor;

    protected long id;

    protected NodeState nodeState;
    protected boolean nodeIsAddedInThisTx;

    public TxAbstractNodeCursor( TransactionState state, Consumer<TxAbstractNodeCursor> cache )
    {
        this.state = state;
        this.cache = cache;
    }

    public TxAbstractNodeCursor init( NodeCursor nodeCursor )
    {
        this.cursor = nodeCursor;
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
    public long getId()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return id;
    }

    @Override
    public LabelCursor labels()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return state.augmentLabelCursor( nodeIsAddedInThisTx ? LabelCursor.EMPTY : cursor.labels(), nodeState );
    }

    @Override
    public PropertyCursor properties()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return state.augmentPropertyCursor( nodeIsAddedInThisTx ? PropertyCursor.EMPTY : cursor.properties(),
                nodeState );
    }

    @Override
    public RelationshipCursor relationships( Direction direction, int... relTypes )
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return state.augmentNodeRelationshipCursor(
                nodeIsAddedInThisTx ? RelationshipCursor.EMPTY : cursor.relationships( direction, relTypes ), nodeState,
                direction, relTypes );
    }

    @Override
    public RelationshipCursor relationships( Direction direction )
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return state.augmentNodeRelationshipCursor(
                nodeIsAddedInThisTx ? RelationshipCursor.EMPTY : cursor.relationships( direction ), nodeState,
                direction, null );
    }
}
