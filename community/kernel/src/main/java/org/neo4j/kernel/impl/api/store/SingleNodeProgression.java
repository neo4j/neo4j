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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;

import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class SingleNodeProgression implements NodeProgression
{
    private final ReadableTransactionState state;
    private long nodeId;

    public SingleNodeProgression( long nodeId, ReadableTransactionState state )
    {
        this.state = state;
        this.nodeId = nodeId;
    }

    @Override
    public boolean nextBatch( Batch batch )
    {
        if ( nodeId != NO_SUCH_NODE )
        {
            batch.init( nodeId, nodeId );
            nodeId = NO_SUCH_NODE;
            return true;
        }
        else
        {
            batch.nothing();
            return false;
        }
    }

    @Override
    public Iterator<Long> addedNodes()
    {
        return null;
    }

    @Override
    public boolean fetchFromTxState( long id )
    {
        return state.nodeIsAddedInThisTx( id );
    }

    @Override
    public boolean fetchFromDisk( long id )
    {
        return !state.nodeIsDeletedInThisTx( id );
    }

    @Override
    public NodeState nodeState( long id )
    {
        return state.getNodeState( id );
    }
}
