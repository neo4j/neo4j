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

import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.txstate.TransactionState;

/**
 * Overlays transaction state on a {@link NodeItem} cursor.
 */
public class TxIteratorNodeCursor
        extends TxAbstractNodeCursor
{
    private Iterator<Long> added;
    private Iterator<Long> addedNodeIterator;

    public TxIteratorNodeCursor( TransactionState state, Consumer<TxIteratorNodeCursor> cache )
    {
        super( state, (Consumer) cache );
    }

    public TxIteratorNodeCursor init( Cursor<NodeItem> nodeCursor, Iterator<Long> addedNodeIterator )
    {
        super.init( nodeCursor );

        this.nodeIsAddedInThisTx = false;
        this.addedNodeIterator = addedNodeIterator;

        added = null;

        return this;
    }

    @Override
    public boolean next()
    {
        if ( added == null )
        {
            while ( cursor.next() )
            {
                id = cursor.get().id();


                if ( state.nodeIsDeletedInThisTx( id ) )
                {
                    id = -1;
                    continue;
                }

                this.nodeState = state.getNodeState( cursor.get().id() );
                return true;
            }

            added = addedNodeIterator;
            nodeIsAddedInThisTx = true;
        }

        if ( added.hasNext() )
        {
            id = added.next();
            this.nodeState = state.getNodeState( id );
            return true;
        }
        else
        {
            this.id = -1;
            this.nodeState = null;
            return false;
        }
    }

}
