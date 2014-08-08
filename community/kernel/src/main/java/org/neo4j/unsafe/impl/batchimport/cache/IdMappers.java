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
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

public class IdMappers
{
    /**
     * An {@link IdMapper} that doesn't touch the input ids, but just asserts that node ids arrive in ascending order.
     */
    public static IdMapper actualIds()
    {
        return new ActualIdMapper();
    }

    public static class ActualIdMapper implements IdMapper
    {
        @Override
        public Iterator<InputNode> wrapNodes( Iterator<InputNode> nodes )
        {
            return new IteratorWrapper<InputNode, InputNode>( nodes )
            {
                private long lastSeenId = -1;

                @Override
                protected InputNode underlyingObjectToObject( InputNode node )
                {
                    if ( lastSeenId != -1 && node.id() < lastSeenId )
                    {
                        throw new IllegalArgumentException( "Cannot go backwards in node id sequence, last seen was " +
                                lastSeenId + ", given id is " + node.id() );
                    }
                    lastSeenId = node.id();
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
}
