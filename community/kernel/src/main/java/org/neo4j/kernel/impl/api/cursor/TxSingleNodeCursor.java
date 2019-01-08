/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.txstate.NodeState;

import static org.neo4j.collection.primitive.Primitive.intSet;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

/**
 * Overlays transaction state on a {@link NodeItem} cursor.
 */
public class TxSingleNodeCursor implements Cursor<NodeItem>, NodeItem
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
    public boolean isDense()
    {
        return cursor.get().isDense();
    }

    @Override
    public long nextGroupId()
    {
        return nodeIsAddedInThisTx ? NO_NEXT_RELATIONSHIP.longValue() : cursor.get().nextGroupId();
    }

    @Override
    public long nextRelationshipId()
    {
        return nodeIsAddedInThisTx ? NO_NEXT_RELATIONSHIP.longValue() : cursor.get().nextRelationshipId();
    }

    @Override
    public long nextPropertyId()
    {
        return nodeIsAddedInThisTx ? NO_NEXT_PROPERTY.longValue() : cursor.get().nextPropertyId();
    }

    @Override
    public Lock lock()
    {
        return nodeIsAddedInThisTx ? NO_LOCK : cursor.get().lock();
    }
}
