/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.cypher;

import org.neo4j.graphdb.ExecutionPlanDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** Takes execution plans and converts them to the subset of types used in the Neo4j type system */
class ExecutionPlanConverter
{
    public static Map<String, Object> convert( ExecutionPlanDescription plan )
    {
        Map<String, Object> out = new HashMap<>();
        out.put( "operatorType", plan.getName() );
        out.put( "args", plan.getArguments() );
        out.put( "identifiers", plan.getIdentifiers() );
        out.put( "children", children( plan ) );
        if ( plan.hasProfilerStatistics() )
        {
            ExecutionPlanDescription.ProfilerStatistics profile = plan.getProfilerStatistics();
            out.put( "dbHits", profile.getDbHits() );
            out.put( "rows", profile.getRows() );
        }
        return out;
    }

    private static List<Map<String,Object>> children( ExecutionPlanDescription plan )
    {
        List<Map<String, Object>> children = new LinkedList<>();
        for ( ExecutionPlanDescription child : plan.getChildren() )
        {
            children.add( convert( child ) );
        }
        return children;
    }
}
