/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.FirstItemIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorWrapper;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.pipes.util.structures.Table;

public class ObjectToRepresentationConverter
{
    public static Representation convert( final Object data )
    {
        if ( data instanceof Table )
        {
            return new GremlinTableRepresentation( (Table) data );
        }
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

    static Representation getIteratorRepresentation( Iterator data )
    {
        final FirstItemIterable<Representation> results = new FirstItemIterable<Representation>(new IteratorWrapper<Representation, Object>(data) {
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
        return new ListRepresentation( getType( results ), results );
    }

    static FirstItemIterable<Representation> convertValuesToRepresentations( Iterable data )
    {
        if ( data instanceof Table )
        {
            return new FirstItemIterable<Representation>(Collections.<Representation>singleton(new GremlinTableRepresentation( (Table) data )));
        }
        return new FirstItemIterable<Representation>(new IterableWrapper<Representation,Object>(data) {
            @Override
            protected Representation underlyingObjectToObject(Object value) {
                if ( value instanceof Iterable )
                {
                    final FirstItemIterable<Representation> nested = convertValuesToRepresentations((Iterable) value);
                    return new ListRepresentation( getType( nested ), nested);
                }
                else
                {
                    return getSingleRepresentation( value );
                }
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
        if ( result == null ) return ValueRepresentation.string( "null" );
        if ( result instanceof Neo4jVertex )
        {
            return new NodeRepresentation(
                    ( (Neo4jVertex) result ).getRawVertex() );
        }
        else if ( result instanceof Neo4jEdge )
        {
            return new RelationshipRepresentation(
                    ( (Neo4jEdge) result ).getRawEdge() );
        }
        else if ( result instanceof GraphDatabaseService )
        {
            return new DatabaseRepresentation( ( (GraphDatabaseService) result ) );
        }
        else if ( result instanceof Node )
        {
            return new NodeRepresentation( (Node) result );
        }
        else if ( result instanceof Relationship )
        {
            return new RelationshipRepresentation( (Relationship) result );
        }
        else if ( result instanceof Neo4jGraph )
        {
            return ValueRepresentation.string( ( (Neo4jGraph) result ).getRawGraph().toString() );
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
        else
        {
            return ValueRepresentation.string( result.toString() );
        }
    }
}
