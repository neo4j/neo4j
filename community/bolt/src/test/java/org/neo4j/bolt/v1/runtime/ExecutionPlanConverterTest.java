/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;

import static org.junit.Assert.assertEquals;

public class ExecutionPlanConverterTest
{

    @Test
    public void profileStatisticConversion() throws Exception
    {
        Map<String,Object> convertedMap = ExecutionPlanConverter.convert(
                new TestExecutionPlanDescription( "description", getProfilerStatistics(), getIdentifiers(),
                        getArguments() ) );
        assertEquals( convertedMap.get( "operatorType" ), "description" );
        assertEquals( convertedMap.get( "args" ), getArguments() );
        assertEquals( convertedMap.get( "identifiers" ), getIdentifiers() );
        assertEquals( convertedMap.get( "children" ), Collections.emptyList() );
        assertEquals( convertedMap.get( "rows" ), 1L );
        assertEquals( convertedMap.get( "dbHits" ), 2L );
        assertEquals( convertedMap.get( "pageCacheHits" ), 3L );
        assertEquals( convertedMap.get( "pageCacheMisses" ), 2L );
        assertEquals( convertedMap.size(), 8 );
    }

    private Map<String,Object> getArguments()
    {
        return MapUtil.map( "argKey", "argValue" );
    }

    private Set<String> getIdentifiers()
    {
        return Iterators.asSet("identifier1", "identifier2");
    }

    private TestProfilerStatistics getProfilerStatistics()
    {
        return new TestProfilerStatistics( 1, 2, 3, 2 );
    }

    private class TestExecutionPlanDescription implements ExecutionPlanDescription
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

    private class TestProfilerStatistics implements ExecutionPlanDescription.ProfilerStatistics
    {

        private final long rows;
        private final long dbHits;
        private final long pageCacheHits;
        private final long pageCacheMisses;

        private TestProfilerStatistics( long rows, long dbHits, long pageCacheHits, long pageCacheMisses )
        {
            this.rows = rows;
            this.dbHits = dbHits;
            this.pageCacheHits = pageCacheHits;
            this.pageCacheMisses = pageCacheMisses;
        }

        @Override
        public long getRows()
        {
            return rows;
        }

        @Override
        public long getDbHits()
        {
            return dbHits;
        }

        @Override
        public long getPageCacheHits()
        {
            return pageCacheHits;
        }

        @Override
        public long getPageCacheMisses()
        {
            return pageCacheMisses;
        }
    }
}
