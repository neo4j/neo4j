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

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.webadmin.rest.representations.JmxAttributeRepresentationDispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CypherResultRepresentation extends ObjectRepresentation
{
    private final Representation resultRepresentation;
    private final ListRepresentation columns;


    public CypherResultRepresentation( ExecutionResult result )
    {
        super( RepresentationType.STRING );
        resultRepresentation = createResultRepresentation(result);
        columns = ListRepresentation.string( result.columns() );
    }

    @Mapping( "columns" )
    public Representation columns()
    {
        return columns;
    }

    @Mapping( "data" )
    public Representation data()
    {
        return resultRepresentation;

    }

    private Representation createResultRepresentation(ExecutionResult executionResult) {
        // rows
        List<Representation> rows = new ArrayList<Representation>();
        for ( Map<String, Object> row : executionResult)
        {
            List<Representation> fields = new ArrayList<Representation>();
            // columns
            for ( String column : executionResult.columns() )
            {
                Representation rowRep = getRepresentation( row.get( column ) );
                fields.add( rowRep );
            }
            rows.add( new ListRepresentation( "row", fields ) );
        }
        return new ListRepresentation( "data", rows );
    }

    Representation getRepresentation( Object r )
    {
        if( r == null )
        {
            return ValueRepresentation.string( null );
        }

        if ( r instanceof Path )
        {
            return new PathRepresentation<Path>((Path) r );
        }

        if(r instanceof Iterable)
        {
            return handleIterable( (Iterable) r );
        }

        if ( r instanceof Node)
        {
            return new NodeRepresentation( (Node) r );
        }

        if ( r instanceof Relationship)
        {
            return new RelationshipRepresentation( (Relationship) r );
        }

        JmxAttributeRepresentationDispatcher representationDispatcher = new JmxAttributeRepresentationDispatcher();
        return representationDispatcher.dispatch( r, "" );
    }

    Representation handleIterable( Iterable data ) {
        final List<Representation> results = new ArrayList<Representation>();
        for ( final Object value : data )
        {
            Representation rep = getRepresentation(value);
            results.add(rep);
        }

        RepresentationType representationType = getType(results);
        return new ListRepresentation( representationType, results );
    }

    RepresentationType getType( List<Representation> representations )
    {
        if ( representations == null || representations.isEmpty() )
            return RepresentationType.STRING;
        return representations.get( 0 ).getRepresentationType();
    }
}