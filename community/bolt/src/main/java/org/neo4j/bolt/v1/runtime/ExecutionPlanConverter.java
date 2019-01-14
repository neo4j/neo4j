/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.runtime;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

/** Takes execution plans and converts them to the subset of types used in the Neo4j type system */
class ExecutionPlanConverter
{
    private ExecutionPlanConverter()
    {
    }

    public static MapValue convert( ExecutionPlanDescription plan )
    {
        Map<String,AnyValue> out = new HashMap<>();
        out.put( "operatorType", stringValue( plan.getName() ) );
        out.put( "args", ValueUtils.asMapValue( plan.getArguments() ) );
        out.put( "identifiers", ValueUtils.asListValue( plan.getIdentifiers() ) );
        out.put( "children", children( plan ) );
        if ( plan.hasProfilerStatistics() )
        {
            ExecutionPlanDescription.ProfilerStatistics profile = plan.getProfilerStatistics();
            out.put( "dbHits", longValue( profile.getDbHits() ) );
            out.put( "pageCacheHits", longValue( profile.getPageCacheHits() ) );
            out.put( "pageCacheMisses", longValue( profile.getPageCacheMisses() ) );
            out.put( "pageCacheHitRatio", doubleValue( profile.getPageCacheHitRatio() ) );
            out.put( "rows", longValue( profile.getRows() ) );
        }
        return VirtualValues.map( out );
    }

    private static ListValue children( ExecutionPlanDescription plan )
    {
        List<AnyValue> children = new LinkedList<>();
        for ( ExecutionPlanDescription child : plan.getChildren() )
        {
            children.add( convert( child ) );
        }
        return VirtualValues.fromList( children );
    }
}
