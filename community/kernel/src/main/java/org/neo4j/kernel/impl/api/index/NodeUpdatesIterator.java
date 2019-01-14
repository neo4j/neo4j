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
package org.neo4j.kernel.impl.api.index;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

class NodeUpdatesIterator implements Iterator<NodeUpdates>
{

    private final IndexStoreView storeView;
    private final PrimitiveLongIterator nodeIdIterator;
    private NodeUpdates nextUpdate;

    NodeUpdatesIterator( IndexStoreView storeView, PrimitiveLongIterator nodeIdIterator )
    {
        this.storeView = storeView;
        this.nodeIdIterator = nodeIdIterator;
    }

    @Override
    public boolean hasNext()
    {
        if ( nextUpdate == null )
        {
            while ( nodeIdIterator.hasNext() )
            {
                long nodeId = nodeIdIterator.next();
                NodeUpdates updates = storeView.nodeAsUpdates( nodeId );
                if ( updates != null )
                {
                    nextUpdate = updates;
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    @Override
    public NodeUpdates next()
    {
        NodeUpdates update = null;
        if ( hasNext() )
        {
            update = this.nextUpdate;
            this.nextUpdate = null;
        }
        return update;
    }
}
