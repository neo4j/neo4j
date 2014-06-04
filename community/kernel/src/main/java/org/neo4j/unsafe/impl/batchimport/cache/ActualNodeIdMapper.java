/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.Iterator;

import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * Treats the ids as actual entity ids in the store.
 */
public class ActualNodeIdMapper implements NodeIdMapper
{
    private long lastSeenNodeId = -1;
    private final NodeStore nodeStore;

    public ActualNodeIdMapper( NodeStore nodeStore )
    {
        this.nodeStore = nodeStore;
    }

    @Override
    public Iterator<InputNode> wrapNodes( Iterator<InputNode> nodes )
    {
        return new IteratorWrapper<InputNode, InputNode>( nodes )
        {
            @Override
            protected InputNode underlyingObjectToObject( InputNode node )
            {
                if ( lastSeenNodeId != -1 && node.id() < lastSeenNodeId )
                {
                    throw new IllegalArgumentException( "Cannot go backwards in node id sequence, last seen was " +
                            lastSeenNodeId + ", given id is " + node.id() );
                }
                nodeStore.setHighId( node.id() + 1 );
                lastSeenNodeId = node.id();
                return node;
            }
        };
    }

    @Override
    public Iterator<InputRelationship> wrapRelationships( Iterator<InputRelationship> relationships )
    {
        return relationships;
    }
}
