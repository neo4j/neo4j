/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.javacompat.PlanDescription;
import org.neo4j.cypher.javacompat.ProfilerStatistics;
import org.neo4j.function.Function;
import org.neo4j.helpers.collection.IterableWrapper;

import java.util.Map;

import static org.neo4j.server.rest.repr.ObjectToRepresentationConverter.getMapRepresentation;

/**
 * This takes a function that resolves to a {@link PlanDescription}, and it does so for two reasons:
 *  - The plan description needs to be fetched *after* the result is streamed to the user
 *  - This method is recursive, so it's not enough to just pass in the execution plan to the root call of it
 *    subsequent inner calls could not re-use that execution plan (that would just lead to an infinite loop)
 */
public abstract class CypherPlanRepresentation extends MappingRepresentation {

    private CypherPlanRepresentation() {
        super("plan");
    }

    protected abstract PlanDescription getPlan();

    @Override
    protected void serialize( MappingSerializer mappingSerializer )
    {
        final PlanDescription planDescription = getPlan();

        mappingSerializer.putString( "name", planDescription.getName() );
        Map<String, Object> arguments = planDescription.getArguments();
        MappingRepresentation argsRepresentation = getMapRepresentation( arguments );
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
                                return newFromPlan(childPlan);
                            }
                        }
                )
        );
    }

    public static CypherPlanRepresentation newFromProvider( final Function<Object, PlanDescription> planProvider ) {
        return new CypherPlanRepresentation() {
            private PlanDescription plan = null;
            private boolean fetched = false;


            @Override
            protected PlanDescription getPlan() {
                if (!fetched) {
                    plan = planProvider.apply( null );
                    fetched = true;
                }
                return plan;
            }
        };
    }

    public static CypherPlanRepresentation newFromPlan( final PlanDescription plan ) {
        return new CypherPlanRepresentation() {
            @Override
            protected PlanDescription getPlan() {
                return plan;
            }
        };
    }
}
