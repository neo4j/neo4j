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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class ExecutionPlanConverterTest
{

    @Test
    void fullProfileStatisticConversion()
    {
        MapValue convertedMap = ExecutionPlanConverter.convert(
                new TestExecutionPlanDescription( "description", getFullProfilerStatistics(), getIdentifiers(),
                                                  getArguments() ) );
        assertEquals( convertedMap.get( "operatorType" ), stringValue( "description" ) );
        assertEquals( convertedMap.get( "args" ), ValueUtils.asMapValue( getArguments() ) );
        assertEquals( convertedMap.get( "identifiers" ), ValueUtils.asListValue( getIdentifiers() ) );
        assertEquals( convertedMap.get( "children" ), VirtualValues.EMPTY_LIST );
        assertEquals( convertedMap.get( "rows" ), longValue( 1L ) );
        assertEquals( convertedMap.get( "dbHits" ), longValue( 2L ) );
        assertEquals( convertedMap.get( "pageCacheHits" ), longValue( 3L ) );
        assertEquals( convertedMap.get( "pageCacheMisses" ), longValue( 2L ) );
        assertEquals( convertedMap.get( "time" ), longValue( 5L ) );
        assertEquals( ((DoubleValue) convertedMap.get( "pageCacheHitRatio" )).doubleValue(), 3.0 / 5, 0.0001 );
        assertEquals( convertedMap.size(), 10 );
    }

    @Test
    void partialProfileStatisticConversion()
    {
        MapValue convertedMap = ExecutionPlanConverter.convert(
                new TestExecutionPlanDescription( "description", getPartialProfilerStatistics(), getIdentifiers(),
                                                  getArguments() ) );
        assertEquals( convertedMap.get( "operatorType" ), stringValue( "description" ) );
        assertEquals( convertedMap.get( "args" ), ValueUtils.asMapValue( getArguments() ) );
        assertEquals( convertedMap.get( "identifiers" ), ValueUtils.asListValue( getIdentifiers() ) );
        assertEquals( convertedMap.get( "children" ), VirtualValues.EMPTY_LIST );
        assertEquals( convertedMap.get( "rows" ), longValue( 1L ) );
        assertEquals( convertedMap.get( "pageCacheHits" ), longValue( 3L ) );
        assertEquals( convertedMap.get( "pageCacheMisses" ), longValue( 2L ) );
        assertEquals( ((DoubleValue) convertedMap.get( "pageCacheHitRatio" )).doubleValue(), 3.0 / 5, 0.0001 );
        assertEquals( convertedMap.size(), 8 );
    }

    @Test
    void noStatisticConversion()
    {
        MapValue convertedMap = ExecutionPlanConverter.convert(
                new TestExecutionPlanDescription( "description", null, getIdentifiers(),
                                                  getArguments() ) );
        assertEquals( convertedMap.get( "operatorType" ), stringValue( "description" ) );
        assertEquals( convertedMap.get( "args" ), ValueUtils.asMapValue( getArguments() ) );
        assertEquals( convertedMap.get( "identifiers" ), ValueUtils.asListValue( getIdentifiers() ) );
        assertEquals( convertedMap.get( "children" ), VirtualValues.EMPTY_LIST );
        assertEquals( convertedMap.size(), 4 );
    }

    private Map<String,Object> getArguments()
    {
        return MapUtil.map( "argKey", "argValue" );
    }

    private Set<String> getIdentifiers()
    {
        return Iterators.asSet( "identifier1", "identifier2" );
    }

    private TestProfilerStatistics getFullProfilerStatistics()
    {
        EnumMap<ProfilerArguments,Long> arguments = new EnumMap<>( ProfilerArguments.class );
        arguments.put( ProfilerArguments.ROWS, 1L );
        arguments.put( ProfilerArguments.DBHITS, 2L );
        arguments.put( ProfilerArguments.PAGE_CACHE_HITS, 3L );
        arguments.put( ProfilerArguments.PAGE_CACHE_MISSES, 2L );
        arguments.put( ProfilerArguments.TIME, 5L );
        return new TestProfilerStatistics( arguments );
    }

    private TestProfilerStatistics getPartialProfilerStatistics()
    {
        EnumMap<ProfilerArguments,Long> arguments = new EnumMap<>( ProfilerArguments.class );
        arguments.put( ProfilerArguments.ROWS, 1L );
        arguments.put( ProfilerArguments.PAGE_CACHE_HITS, 3L );
        arguments.put( ProfilerArguments.PAGE_CACHE_MISSES, 2L );
        return new TestProfilerStatistics( arguments );
    }

    private static class TestExecutionPlanDescription implements ExecutionPlanDescription
    {

        private final String name;
        private final ProfilerStatistics profilerStatistics;
        private final Set<String> identifiers;
        private final Map<String,Object> arguments;

        TestExecutionPlanDescription( String name, ProfilerStatistics profilerStatistics, Set<String> identifiers,
                                      Map<String,Object> arguments )
        {
            this.name = name;
            this.profilerStatistics = profilerStatistics;
            this.identifiers = identifiers;
            this.arguments = arguments;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public List<ExecutionPlanDescription> getChildren()
        {
            return Collections.emptyList();
        }

        @Override
        public Map<String,Object> getArguments()
        {
            return arguments;
        }

        @Override
        public Set<String> getIdentifiers()
        {
            return identifiers;
        }

        @Override
        public boolean hasProfilerStatistics()
        {
            return profilerStatistics != null;
        }

        @Override
        public ProfilerStatistics getProfilerStatistics()
        {
            return profilerStatistics;
        }
    }

    private enum ProfilerArguments
    {
        ROWS,
        DBHITS,
        PAGE_CACHE_HITS,
        PAGE_CACHE_MISSES,
        TIME
    }

    private static class TestProfilerStatistics implements ExecutionPlanDescription.ProfilerStatistics
    {

        private final Map<ProfilerArguments,Long> arguments;

        private TestProfilerStatistics( Map<ProfilerArguments,Long> arguments )
        {
            this.arguments = arguments;
        }

        @Override
        public boolean hasRows()
        {
            return arguments.containsKey( ProfilerArguments.ROWS );
        }

        @Override
        public boolean hasDbHits()
        {
            return arguments.containsKey( ProfilerArguments.DBHITS );
        }

        @Override
        public boolean hasPageCacheStats()
        {
            return arguments.containsKey( ProfilerArguments.PAGE_CACHE_HITS )
                   && arguments.containsKey( ProfilerArguments.PAGE_CACHE_MISSES );
        }

        @Override
        public boolean hasTime()
        {
            return arguments.containsKey( ProfilerArguments.TIME );
        }

        @Override
        public long getRows()
        {
            return arguments.get( ProfilerArguments.ROWS );
        }

        @Override
        public long getDbHits()
        {
            return arguments.get( ProfilerArguments.DBHITS );
        }

        @Override
        public long getPageCacheHits()
        {
            return arguments.get( ProfilerArguments.PAGE_CACHE_HITS );
        }

        @Override
        public long getPageCacheMisses()
        {
            return arguments.get( ProfilerArguments.PAGE_CACHE_MISSES );
        }

        @Override
        public long getTime()
        {
            return arguments.get( ProfilerArguments.TIME );
        }
    }
}
