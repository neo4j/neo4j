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
package org.neo4j.server.rest.domain;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;

// import org.neo4j.graphdb.Traverser.Order;
// import org.neo4j.graphdb.traversal.TraversalDescription;
// import org.neo4j.graphdb.traversal.Uniqueness;
// import org.neo4j.kernel.Traversal;

public class RelationshipExpanderBuilder
{

    @SuppressWarnings( "unchecked" )
    public static RelationshipExpander describeRelationships( Map<String, Object> description )
    {
        Expander expander = Traversal.emptyExpander();

        Object relationshipsDescription = description.get( "relationships" );
        if ( relationshipsDescription != null )
        {
            Collection<Object> pairDescriptions;
            if ( relationshipsDescription instanceof Collection )
            {
                pairDescriptions = (Collection<Object>) relationshipsDescription;
            }
            else
            {
                pairDescriptions = Arrays.asList( relationshipsDescription );
            }

            for ( Object pairDescription : pairDescriptions )
            {
                Map map = (Map) pairDescription;
                String name = (String) map.get( "type" );
                RelationshipType type = DynamicRelationshipType.withName( name );
                String directionName = (String) map.get( "direction" );
                expander = ( directionName == null ) ? expander.add( type ) : expander.add( type,
                        stringToEnum( directionName, RelationshipDirection.class, true ).internal );
            }
        }
        return expander;
    }

    // TODO Refactor - same method exists in TraversalDescriptionBuilder
    private static <T extends Enum<T>> T stringToEnum( String name, Class<T> enumClass, boolean fuzzyMatch )
    {
        if ( name == null )
        {
            return null;
        }

        // name = enumifyName( name );
        for ( T candidate : enumClass.getEnumConstants() )
        {
            if ( candidate.name()
                    .equals( name ) )
            {
                return candidate;
            }
        }
        if ( fuzzyMatch )
        {
            for ( T candidate : enumClass.getEnumConstants() )
            {
                if ( candidate.name()
                        .startsWith( name ) )
                {
                    return candidate;
                }
            }
        }
        throw new RuntimeException( "Unregognized " + enumClass.getSimpleName() + " '" + name + "'" );
    }

}
