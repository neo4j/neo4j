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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.IterableWrapper;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class CypherResultRepresentation extends MappingRepresentation
{
    private static final RepresentationDispatcher REPRESENTATION_DISPATCHER = new CypherRepresentationDispatcher();

    private final ListRepresentation resultRepresentation;
    private final ListRepresentation columns;
    private final MappingRepresentation statsRepresentation;
    private final MappingRepresentation plan;

    public CypherResultRepresentation( final Result result, boolean includeStats, boolean includePlan )
    {
        super( RepresentationType.STRING );
        resultRepresentation = createResultRepresentation( result );
        columns = ListRepresentation.string( result.columns() );
        statsRepresentation = includeStats ? new CypherStatisticsRepresentation( result.getQueryStatistics() ) : null;
        plan = includePlan ? CypherPlanRepresentation.newFromProvider(planProvider(result)) : null;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putList( "columns", columns );
        serializer.putList( "data", resultRepresentation );

        if (statsRepresentation != null)
            serializer.putMapping( "stats", statsRepresentation );
        if (plan != null)
            serializer.putMapping( "plan", plan );
    }

    private ListRepresentation createResultRepresentation( Result executionResult )
    {
        final List<String> columns = executionResult.columns();
        Iterable<Map<String, Object>> inner = new RepresentationExceptionHandlingIterable<>( loop( executionResult ) );
        return new ListRepresentation( "data", new IterableWrapper<Representation,Map<String,Object>>(inner) {

            @Override
            protected Representation underlyingObjectToObject(final Map<String, Object> row) {
                return new ListRepresentation("row",
                 new IterableWrapper<Representation,String>(columns) {

                     @Override
                     protected Representation underlyingObjectToObject(String column) {
                         return getRepresentation( row.get( column ) );
                     }
                 });
            }
        });
    }

    private Representation getRepresentation( Object r )
    {
        if( r == null )
        {
            return ValueRepresentation.string( null );
        }

        if ( r instanceof Path )
        {
            return new PathRepresentation<>((Path) r );
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

        return REPRESENTATION_DISPATCHER.dispatch( r, "" );
    }

    private Representation handleIterable( Iterable data ) {
        final List<Representation> results = new ArrayList<>();
        for ( final Object value : data )
        {
            Representation rep = getRepresentation(value);
            results.add(rep);
        }

        RepresentationType representationType = getType(results);
        return new ListRepresentation( representationType, results );
    }

    private RepresentationType getType( List<Representation> representations )
    {
        if ( representations == null || representations.isEmpty() )
            return RepresentationType.STRING;
        return representations.get( 0 ).getRepresentationType();
    }

    private Function<Object, ExecutionPlanDescription> planProvider( final Result result )
    {
        return new Function<Object,ExecutionPlanDescription>(){
            @Override
            public ExecutionPlanDescription apply( Object from )
            {
                return result.getExecutionPlanDescription();
            }
        };
    }

}
