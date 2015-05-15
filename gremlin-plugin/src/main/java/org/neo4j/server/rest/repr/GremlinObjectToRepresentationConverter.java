/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.pipes.util.structures.Table;
import org.neo4j.helpers.collection.FirstItemIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorWrapper;

public class GremlinObjectToRepresentationConverter
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
            return getIteratorRepresentation( (Iterator) data );
        }
        if ( data instanceof Map )
        {
            return getMapRepresentation( (Map) data );
        }

        return getSingleRepresentation( data );
    }

    public static MappingRepresentation getMapRepresentation( Map data )
    {
        return new GremlinMapRepresentation( data );
    }

    static Representation getIteratorRepresentation( Iterator data )
    {
        final FirstItemIterable<Representation> results = new FirstItemIterable<Representation>( new IteratorWrapper<Representation, Object>( data )
        {
            @Override
            protected Representation underlyingObjectToObject( Object value )
            {
                if ( value instanceof Iterable )
                {
                    FirstItemIterable<Representation> nested = convertValuesToRepresentations( (Iterable) value );
                    return new ListRepresentation( getType( nested ), nested );
                } else
                {
                    return getSingleRepresentation( value );
                }
            }
        } );
        return new ListRepresentation( getType( results ), results );
    }

    public static ListRepresentation getListRepresentation( Iterable data )
    {
        final FirstItemIterable<Representation> results = convertValuesToRepresentations( data );
        return new ServerListRepresentation( getType( results ), results );
    }

    static FirstItemIterable<Representation> convertValuesToRepresentations( Iterable data )
    {
        if ( data instanceof Table )
        {
            return new FirstItemIterable<Representation>( Collections.<Representation>singleton( new GremlinTableRepresentation( (Table) data ) ) );
        }
        return new FirstItemIterable<Representation>( new IterableWrapper<Representation, Object>( data )
        {
            @Override
            protected Representation underlyingObjectToObject( Object value )
            {
                return convert( value );
            }
        } );
    }

    static RepresentationType getType( FirstItemIterable<Representation> representations )
    {
        Representation representation = representations.getFirst();
        if ( representation == null )
        {
            return RepresentationType.STRING;
        }
        return representation.getRepresentationType();
    }

    static Representation getSingleRepresentation( Object result )
    {
        if ( result == null )
        {
            return ObjectToRepresentationConverter.getSingleRepresentation( result );
        }

        if ( result instanceof Neo4jVertex )
        {
            return new NodeRepresentation( ((Neo4jVertex) result).getRawVertex() );
        }
        if ( result instanceof Neo4jEdge )
        {
            return new RelationshipRepresentation( ((Neo4jEdge) result).getRawEdge() );
        }
        if ( result instanceof Neo4jGraph )
        {
            return ValueRepresentation.string( ((Neo4jGraph) result).getRawGraph().toString() );
        }

        return ObjectToRepresentationConverter.getSingleRepresentation( result );
    }
}
