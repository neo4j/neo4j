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
package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.NestingIterator;

public final class OrderedByTypeExpander extends
        StandardExpander.RegularExpander
{
    private final Collection<Pair<RelationshipType, Direction>> orderedTypes;
    
    public OrderedByTypeExpander()
    {
        this( Collections.<Pair<RelationshipType, Direction>>emptyList() );
    }

    OrderedByTypeExpander( Collection<Pair<RelationshipType, Direction>> orderedTypes )
    {
        super( Collections.<Direction, RelationshipType[]>emptyMap() );
        this.orderedTypes = orderedTypes;
    }

    @Override
    public StandardExpander add( RelationshipType type, Direction direction )
    {
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<Pair<RelationshipType,Direction>>( orderedTypes );
        newTypes.add( Pair.of( type, direction ) );
        return new OrderedByTypeExpander( newTypes );
    }
    
    @Override
    public StandardExpander remove( RelationshipType type )
    {
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<Pair<RelationshipType,Direction>>();
        for ( Pair<RelationshipType, Direction> pair : orderedTypes )
            if ( !type.name().equals( pair.first().name() ) )
                newTypes.add( pair );
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
        Collection<Pair<RelationshipType, Direction>> newTypes = new ArrayList<Pair<RelationshipType,Direction>>();
        for ( Pair<RelationshipType, Direction> pair : orderedTypes )
            newTypes.add( Pair.of( pair.first(), pair.other().reverse() ) );
        return new OrderedByTypeExpander( newTypes );
    }

    @Override
    RegularExpander createNew( Map<Direction, RelationshipType[]> newTypes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    Iterator<Relationship> doExpand( final Path path, BranchState state )
    {
        final Node node = path.endNode();
        return new NestingIterator<Relationship, Pair<RelationshipType, Direction>>(
                orderedTypes.iterator() )
        {
            @Override
            protected Iterator<Relationship> createNestedIterator(
                    Pair<RelationshipType, Direction> entry )
            {
                RelationshipType type = entry.first();
                Direction dir = entry.other();
                return ( ( dir == Direction.BOTH ) ? node.getRelationships( type ) :
                        node.getRelationships( type, dir ) ).iterator();
            }
        };
    }
}
