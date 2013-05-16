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

import static org.neo4j.server.rest.repr.ObjectToRepresentationConverter.getMapRepresentation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.QueryStatistics;
import org.neo4j.cypher.javacompat.PlanDescription;
import org.neo4j.cypher.javacompat.ProfilerStatistics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.server.webadmin.rest.representations.JmxAttributeRepresentationDispatcher;

public class CypherResultRepresentation extends MappingRepresentation
{
    private final ListRepresentation resultRepresentation;
    private final ListRepresentation columns;
    private final MappingRepresentation statsRepresentation;
    private final MappingRepresentation plan;


    public CypherResultRepresentation( final ExecutionResult result, boolean includeStats, boolean includePlan )
    {
        super( RepresentationType.STRING );
        resultRepresentation = createResultRepresentation( result );
        columns = ListRepresentation.string( result.columns() );
        statsRepresentation = includeStats ? createStatsRepresentation( result.getQueryStatistics() ) : null;
        plan = includePlan ? createPlanRepresentation( planProvider( result ) ) : null;
    }

    private MappingRepresentation createStatsRepresentation( final QueryStatistics stats )
    {
        return new MappingRepresentation( "stats" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putBoolean( "contains_updates", stats.containsUpdates() );
                serializer.putNumber( "nodes_created", stats.getNodesCreated() );
                serializer.putNumber( "nodes_deleted", stats.getDeletedNodes() );
                serializer.putNumber( "properties_set", stats.getPropertiesSet() );
                serializer.putNumber( "relationships_created", stats.getRelationshipsCreated() );
                serializer.putNumber( "relationship_deleted", stats.getDeletedRelationships() );
                serializer.putNumber( "labels_added", stats.getLabelsAdded() );
                serializer.putNumber( "labels_removed", stats.getLabelsRemoved() );
                serializer.putNumber( "indexes_added", stats.getIndexesAdded() );
                serializer.putNumber( "indexes_removed", stats.getIndexesRemoved() );
                serializer.putNumber( "constraints_added", stats.getConstraintsAdded() );
                serializer.putNumber( "constraints_removed", stats.getConstraintsRemoved() );
            }
        };
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

    private ListRepresentation createResultRepresentation(ExecutionResult executionResult) {
        final List<String> columns = executionResult.columns();
        final Iterable<Map<String, Object>> inner = new RepresentationExceptionHandlingIterable<Map<String,Object>>(executionResult);
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

    /*
     * This takes a function that resolves to a {@link PlanDescription}, and it does so for two reasons:
     *  - The plan description needs to be fetched *after* the result is streamed to the user
     *  - This method is recursive, so it's not enough to just pass in the executionplan to the root call of it
     *    subsequent inner calls could not re-use that execution plan (that would just lead to an infinite loop)
     */
    private MappingRepresentation createPlanRepresentation( final Function<Object, PlanDescription> getPlan )
    {
        return new MappingRepresentation( "plan" ) {
            @Override
            protected void serialize( MappingSerializer mappingSerializer )
            {
                final PlanDescription planDescription = getPlan.apply( null );

                mappingSerializer.putString( "name", planDescription.getName() );
                MappingRepresentation argsRepresentation = getMapRepresentation( (Map) planDescription.getArguments() );
                mappingSerializer.putMapping( "args", argsRepresentation );

                if ( planDescription.hasProfilerStatistics() )
                {
                    ProfilerStatistics stats = planDescription.getProfilerStatistics();
                    mappingSerializer.putNumber( "rows", stats.getRows() );
                    mappingSerializer.putNumber( "dbHits", stats.getDbHits() );
                }

                mappingSerializer.putList( "children",
                        new ListRepresentation( "children",
                                new IterableWrapper<Representation, PlanDescription>(planDescription.getChildren()) {

                                    @Override
                                    protected Representation underlyingObjectToObject( final PlanDescription childPlan )
                                    {
                                        return createPlanRepresentation( new Function<Object, PlanDescription>(){

                                            @Override
                                            public PlanDescription apply( Object from )
                                            {
                                                return childPlan;
                                            }
                                        });
                                    }
                                }
                        )
                );
            }
        };
    }

    private Representation getRepresentation( Object r )
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

    private Representation handleIterable( Iterable data ) {
        final List<Representation> results = new ArrayList<Representation>();
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

    private Function<Object, PlanDescription> planProvider( final ExecutionResult result )
    {
        return new Function<Object,PlanDescription>(){
            @Override
            public PlanDescription apply( Object from )
            {
                return result.executionPlanDescription();
            }
        };
    }

}