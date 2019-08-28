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
package org.neo4j.server.rest.repr;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.FirstItemIterable;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.internal.helpers.collection.IteratorWrapper;

public class ObjectToRepresentationConverter
{
    public static Representation convert( final Object data, GraphDatabaseService databaseService )
    {
        if ( data instanceof Iterable )
        {
            return getListRepresentation( (Iterable) data, databaseService );
        }
        if ( data instanceof Iterator )
        {
            Iterator iterator = (Iterator) data;
            return getIteratorRepresentation( iterator, databaseService );
        }
        if ( data instanceof Map )
        {
            return getMapRepresentation( (Map) data, databaseService );
        }
        return getSingleRepresentation( data, databaseService );
    }

    private ObjectToRepresentationConverter()
    {
    }

    public static MappingRepresentation getMapRepresentation( Map data, GraphDatabaseService databaseService )
    {
        return new MapRepresentation( data, databaseService );
    }

    @SuppressWarnings( "unchecked" )
    static Representation getIteratorRepresentation( Iterator data, GraphDatabaseService databaseService )
    {
        final FirstItemIterable<Representation> results =
                new FirstItemIterable<>( new IteratorWrapper<Representation,Object>( data )
                {
                    @Override
                    protected Representation underlyingObjectToObject( Object value )
                    {
                        if ( value instanceof Iterable )
                        {
                            FirstItemIterable<Representation> nested = convertValuesToRepresentations( (Iterable) value, databaseService );
                            return new ListRepresentation( getType( nested ), nested );
                        }
                        else
                        {
                            return getSingleRepresentation( value, databaseService );
                        }
                    }
                } );
        return new ListRepresentation( getType( results ), results );
    }

    public static ListRepresentation getListRepresentation( Iterable data, GraphDatabaseService databaseService )
    {
        final FirstItemIterable<Representation> results = convertValuesToRepresentations( data, databaseService );
        return new ServerListRepresentation( getType( results ), results, databaseService );
    }

    @SuppressWarnings( "unchecked" )
    static FirstItemIterable<Representation> convertValuesToRepresentations( Iterable data, GraphDatabaseService databaseService )
    {
        return new FirstItemIterable<>( new IterableWrapper<Representation,Object>( data )
        {
            @Override
            protected Representation underlyingObjectToObject( Object value )
            {
                return convert( value, databaseService );
            }
        } );
    }

    static RepresentationType getType( FirstItemIterable<Representation> representations )
    {
        Representation  representation = representations.getFirst();
        if ( representation == null )
        {
            return RepresentationType.STRING;
        }
        return representation.getRepresentationType();
    }

    static Representation getSingleRepresentation( Object result, GraphDatabaseService databaseService )
    {
        if ( result == null )
        {
            return ValueRepresentation.ofNull();
        }
        else if ( result instanceof Node )
        {
            return new NodeRepresentation( (Node) result, databaseService );
        }
        else if ( result instanceof Relationship )
        {
            return new RelationshipRepresentation( (Relationship) result, databaseService );
        }
        else if ( result instanceof Double || result instanceof Float )
        {
            return ValueRepresentation.number( ( (Number) result ).doubleValue() );
        }
        else if ( result instanceof Long )
        {
            return ValueRepresentation.number( ( (Long) result ).longValue() );
        }
        else if ( result instanceof Integer )
        {
            return ValueRepresentation.number( ( (Integer) result ).intValue() );
        }
        else if ( result instanceof Boolean )
        {
            return ValueRepresentation.bool( ( (Boolean) result ).booleanValue()  );
        }
        else
        {
            return ValueRepresentation.string( result.toString() );
        }
    }
}
