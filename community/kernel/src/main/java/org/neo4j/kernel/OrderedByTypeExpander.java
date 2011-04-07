/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.NestingIterator;

public final class OrderedByTypeExpander extends
        StandardExpander.RegularExpander
{
    public OrderedByTypeExpander()
    {
        this( new RelationshipType[0],
                Collections.<String, Direction>emptyMap() );
    }

    OrderedByTypeExpander( RelationshipType[] types,
            Map<String, Direction> directions )
    {
        super( types, directions );
    }

    @Override
    RegularExpander createNew( RelationshipType[] newTypes,
            Map<String, Direction> newDirections )
    {
        return new OrderedByTypeExpander( newTypes, newDirections );
    }

    @Override
    Iterator<Relationship> doExpand( final Node start )
    {
        return new NestingIterator<Relationship, RelationshipType>(
                new ArrayIterator<RelationshipType>( types ) )
        {
            @Override
            protected Iterator<Relationship> createNestedIterator(
                    RelationshipType type )
            {
                Direction dir = directions.get( type.name() );
                dir = ( dir == null ) ? Direction.BOTH : dir;
                return ( ( dir == Direction.BOTH ) ? start.getRelationships( type )
                        : start.getRelationships( type, dir ) ).iterator();
            }
        };
    }
}
