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

import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.helpers.collection.IterableWrapper;

import static org.neo4j.server.rest.repr.ObjectToRepresentationConverter.getMapRepresentation;

/**
 * This takes a function that resolves to a {@link org.neo4j.graphdb.ExecutionPlanDescription}, and it does so for two reasons:
 *  - The plan description needs to be fetched *after* the result is streamed to the user
 *  - This method is recursive, so it's not enough to just pass in the execution plan to the root call of it
 *    subsequent inner calls could not re-use that execution plan (that would just lead to an infinite loop)
 */
public abstract class CypherPlanRepresentation extends MappingRepresentation
{

    private CypherPlanRepresentation()
    {
        super( "plan" );
    }

    protected abstract ExecutionPlanDescription getPlan();

    @Override
    protected void serialize( MappingSerializer mappingSerializer )
    {
        final ExecutionPlanDescription planDescription = getPlan();

        mappingSerializer.putString( "name", planDescription.getName() );
        Map<String, Object> arguments = planDescription.getArguments();
        MappingRepresentation argsRepresentation = getMapRepresentation( arguments );
        mappingSerializer.putMapping( "args", argsRepresentation );

        if ( planDescription.hasProfilerStatistics() )
        {
            ExecutionPlanDescription.ProfilerStatistics stats = planDescription.getProfilerStatistics();
            mappingSerializer.putNumber( "rows", stats.getRows() );
            mappingSerializer.putNumber( "dbHits", stats.getDbHits() );
        }

        mappingSerializer.putList( "children",
                new ListRepresentation( "children",
                        new IterableWrapper<Representation, ExecutionPlanDescription>( planDescription.getChildren() )
                        {
                            @Override
                            protected Representation underlyingObjectToObject( final ExecutionPlanDescription childPlan )
                            {
                                return newFromPlan( childPlan );
                            }
                        }
                )
        );
    }

    public static CypherPlanRepresentation newFromProvider( final Function<Object, ExecutionPlanDescription> planProvider )
    {
        return new CypherPlanRepresentation()
        {
            private ExecutionPlanDescription plan = null;
            private boolean fetched = false;


            @Override
            protected ExecutionPlanDescription getPlan()
            {
                if ( !fetched )
                {
                    plan = planProvider.apply( null );
                    fetched = true;
                }
                return plan;
            }
        };
    }

    public static CypherPlanRepresentation newFromPlan( final ExecutionPlanDescription plan )
    {
        return new CypherPlanRepresentation()
        {
            @Override
            protected ExecutionPlanDescription getPlan()
            {
                return plan;
            }
        };
    }
}
