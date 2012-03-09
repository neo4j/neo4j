/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class CypherResultRepresentation extends ObjectRepresentation
{

    private final ExecutionResult queryResult;

    public CypherResultRepresentation( ExecutionResult result )
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
    }

    @Mapping( "columns" )
    public Representation columns()
    {

        return ListRepresentation.string( queryResult.columns() );
    }

    @Mapping( "data" )
    public Representation data()
    {
        // rows
        List<Representation> rows = new ArrayList<Representation>();
        for ( Map<String, Object> row : queryResult )
        {
            List<Representation> fields = new ArrayList<Representation>();
            // columns
            for ( String column : queryResult.columns() )
            {
                Representation rowRep = getRepresentation( row.get( column ) );
                fields.add( rowRep );

            }
            rows.add( new ListRepresentation( "row", fields ) );
        }
        return new ListRepresentation( "data", rows );
    }

    private Representation getRepresentation( Object r )
    {
        if(r == null ) {
            return ValueRepresentation.string( null );
        }
        if ( r instanceof Node )
        {
            return new NodeRepresentation( (Node) r );
        }
        if ( r instanceof Relationship )
        {
            return new RelationshipRepresentation( (Relationship) r );
        }
        else if ( r instanceof Double || r instanceof Float )
        {
            return ValueRepresentation.number( ( (Number) r ).doubleValue() );
        }
        else if ( r instanceof Long || r instanceof Integer )
        {
            return ValueRepresentation.number( ( (Number) r ).longValue() );
        }
        else if ( r instanceof Path )
        {
            return new PathRepresentation<Path>((Path) r );
        }
        else if ( r instanceof Iterable )
        {
            return getListRepresentation( (Iterable) r );
        }
        else
        {
            return ValueRepresentation.string( r.toString() );
        }
    }

    Representation getListRepresentation( Iterable data )
    {
        final List<Representation> results = convertValuesToRepresentations( data );
        return new ListRepresentation( getType( results ), results );
    }

    List<Representation> convertValuesToRepresentations( Iterable data )
    {
        final List<Representation> results = new ArrayList<Representation>();
        for ( final Object value : data )
        {
            if ( value instanceof Iterable )
            {
                List<Representation> nested = new ArrayList<Representation>();
                nested.addAll( convertValuesToRepresentations( (Iterable) value ) );
                results.add( new ListRepresentation( getType( nested ), nested ) );
            }
            else
            {
                results.add( getSingleRepresentation( value ) );
            }
        }
        return results;
    }

    RepresentationType getType( List<Representation> representations )
    {
        if ( representations == null || representations.isEmpty() )
            return RepresentationType.STRING;
        return representations.get( 0 ).getRepresentationType();
    }

    Representation getSingleRepresentation( Object result )
    {
        if ( result == null )
            return ValueRepresentation.string( "null" );
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
        else
        {
            return ValueRepresentation.string( result.toString() );
        }
    }
}
