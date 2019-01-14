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
package org.neo4j.graphdb.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.NestingResourceIterator;
import org.neo4j.helpers.collection.Pair;

public final class OrderedByTypeExpander extends StandardExpander.RegularExpander
{
    private final Collection<Pair<RelationshipType, Direction>> orderedTypes;

    public OrderedByTypeExpander()
    {
        this( Collections.emptyList() );
    }

    private OrderedByTypeExpander( Collection<Pair<RelationshipType,Direction>> orderedTypes )
    {
        super( Collections.emptyMap() );
        this.orderedTypes = orderedTypes;
    }

    @Override
    public StandardExpander add( RelationshipType type, Direction direction )
    {
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<>( orderedTypes );
        newTypes.add( Pair.of( type, direction ) );
        return new OrderedByTypeExpander( newTypes );
    }

    @Override
    public StandardExpander remove( RelationshipType type )
    {
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<>();
        for ( Pair<RelationshipType,Direction> pair : orderedTypes )
        {
            if ( !type.name().equals( pair.first().name() ) )
            {
                newTypes.add( pair );
            }
        }
        return new OrderedByTypeExpander( newTypes );
    }

    @Override
    void buildString( StringBuilder result )
    {
        result.append( orderedTypes.toString() );
    }

    @Override
    public StandardExpander reverse()
    {
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<>();
        for ( Pair<RelationshipType,Direction> pair : orderedTypes )
        {
            newTypes.add( Pair.of( pair.first(), pair.other().reverse() ) );
        }
        return new OrderedByTypeExpander( newTypes );
    }

    @Override
    RegularExpander createNew( Map<Direction, RelationshipType[]> newTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    ResourceIterator<Relationship> doExpand( final Path path, BranchState state )
    {
        final Node node = path.endNode();
        return new NestingResourceIterator<Relationship, Pair<RelationshipType, Direction>>(
                orderedTypes.iterator() )
        {
            @Override
            protected ResourceIterator<Relationship> createNestedIterator(
                    Pair<RelationshipType, Direction> entry )
            {
                RelationshipType type = entry.first();
                Direction dir = entry.other();
                Iterable<Relationship> relationshipsIterable =
                        (dir == Direction.BOTH) ? node.getRelationships( type ) : node.getRelationships( type, dir );
                return Iterables.asResourceIterable( relationshipsIterable ).iterator();
            }
        };
    }
}
