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
package org.neo4j.cypher.javacompat;

import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.cypher.ExtendedPlanDescription;
import org.neo4j.cypher.ProfilerStatisticsNotReadyException;
import org.neo4j.graphdb.ExecutionPlanDescription;

import static org.neo4j.helpers.Exceptions.withCause;

class Description implements ExecutionPlanDescription
{
    private final org.neo4j.cypher.PlanDescription description;

    public Description( org.neo4j.cypher.PlanDescription description )
    {
        this.description = description;
    }

    @Override
    public String toString()
    {
        return description.toString();
    }

    @Override
    public String getName()
    {
        return description.name();
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        List<ExecutionPlanDescription> result = new ArrayList<>();
        for ( org.neo4j.cypher.PlanDescription child : JavaConversions.asJavaIterable( description.children() ) )
        {
            result.add( new Description( child ) );
        }
        return result;
    }

    @Override
    public Map<String, Object> getArguments()
    {
        return JavaConversions.mapAsJavaMap( description.arguments() );
    }

    @Override
    public Set<String> getIdentifiers()
    {
        if ( description instanceof ExtendedPlanDescription )
        {
            return JavaConversions.setAsJavaSet( ((ExtendedPlanDescription) description).identifiers() );
        }
        return Collections.emptySet();
    }

    @Override
    public boolean hasProfilerStatistics()
    {
        return description.hasProfilerStatistics();
    }

    @Override
    public ProfilerStatistics getProfilerStatistics()
    {
        final org.neo4j.cypher.javacompat.ProfilerStatistics statistics;
        try
        {
            statistics = description.asJava().getProfilerStatistics();
        }
        catch ( ProfilerStatisticsNotReadyException e )
        {
            throw withCause( new NoSuchElementException( e.getMessage() ), e );
        }
        return new ProfilerStatistics()
        {
            @Override
            public long getRows()
            {
                return statistics.getRows();
            }

            @Override
            public long getDbHits()
            {
                return statistics.getDbHits();
            }

            @Override
            public String toString()
            {
                return statistics.toString();
            }
        };
    }
}
