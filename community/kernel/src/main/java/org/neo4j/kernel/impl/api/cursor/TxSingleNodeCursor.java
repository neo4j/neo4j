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
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.txstate.TransactionState;

/**
 * Overlays transaction state on a {@link NodeCursor}.
 */
public class TxSingleNodeCursor
        extends TxAbstractNodeCursor
{
    public TxSingleNodeCursor( TransactionState state, Consumer<TxSingleNodeCursor> cache )
    {
        super( state, (Consumer) cache );
    }

    public TxSingleNodeCursor init( NodeCursor nodeCursor )
    {
        super.init( nodeCursor );
        return this;
    }

    @Override
    public boolean next()
    {
        boolean exists = cursor.next();

        id = cursor.getId();

        if ( state.nodeIsDeletedInThisTx( id ) )
        {
            this.id = -1;
            return false;
        }

        this.nodeIsAddedInThisTx = state.nodeIsAddedInThisTx( id );
        if ( exists || nodeIsAddedInThisTx )
        {
            this.nodeState = state.getNodeState( cursor.getId() );
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
