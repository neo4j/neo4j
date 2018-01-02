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
package org.neo4j.server.rest.repr;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.FirstItemIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorWrapper;

public class ObjectToRepresentationConverter
{
    public static Representation convert( final Object data )
    {
        if ( data instanceof Iterable )
        {
            return getListRepresentation( (Iterable) data );
        }
        if ( data instanceof Iterator )
        {
            Iterator iterator = (Iterator) data;
            return getIteratorRepresentation( iterator );
        }
        if ( data instanceof Map )
        {
            
            return getMapRepresentation( (Map) data );
        }
        return getSingleRepresentation( data );
    }

    public static MappingRepresentation getMapRepresentation( Map data )
    {
        
        return new MapRepresentation( data );
    }

    @SuppressWarnings("unchecked")
    static Representation getIteratorRepresentation( Iterator data )
    {
        final FirstItemIterable<Representation> results = new FirstItemIterable<>(new IteratorWrapper<Representation, Object>(data) {
            @Override
            protected Representation underlyingObjectToObject(Object value) {
                if ( value instanceof Iterable )
                {
                    FirstItemIterable<Representation> nested = convertValuesToRepresentations( (Iterable) value );
                    return new ListRepresentation( getType( nested ), nested );
                } else {
                    return getSingleRepresentation( value );
                }
            }
        });
        return new ListRepresentation( getType( results ), results );
    }

    public static ListRepresentation getListRepresentation( Iterable data )
    {
        final FirstItemIterable<Representation> results = convertValuesToRepresentations( data );
        return new ServerListRepresentation( getType( results ), results );
    }

    @SuppressWarnings("unchecked")
    static FirstItemIterable<Representation> convertValuesToRepresentations( Iterable data )
    {
        return new FirstItemIterable<>(new IterableWrapper<Representation,Object>(data) {
            @Override
            protected Representation underlyingObjectToObject(Object value) {
               return convert(value);
            }
        });
    }

    static RepresentationType getType( FirstItemIterable<Representation> representations )
    {
        Representation  representation = representations.getFirst();
        if ( representation == null ) return RepresentationType.STRING;
        return representation.getRepresentationType();
    }

    static Representation getSingleRepresentation( Object result )
    {
        if ( result == null )
        {
            return ValueRepresentation.ofNull();
        }
        else if ( result instanceof GraphDatabaseService )
        {
            return new DatabaseRepresentation();
        }
        else if ( result instanceof Node )
        {
            return new NodeRepresentation( (Node) result );
        }
        else if ( result instanceof Relationship )
        {
            return new RelationshipRepresentation( (Relationship) result );
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
