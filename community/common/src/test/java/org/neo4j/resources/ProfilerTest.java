/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProfilerTest
{
    private static final int COMPUTE_WORK_MILLIS = 10;
    private static final String METHOD = "expensiveComputation";

    @Test
    void profilerMustNoticeWhereTimeGoes() throws Exception
    {
        Profiler profiler = Profiler.profiler();
        try ( Profiler.ProfiledInterval ignored = profiler.profile() )
        {
            expensiveComputation( profiler );
        }
        profiler.finish();
        String output = getProfilerOutput( profiler );
        assertThat( output ).contains( METHOD );
    }

    @Test
    void profilerMustLimitItselfToProfiledRegion() throws Exception
    {
        Profiler profiler = Profiler.profiler();
        try ( Profiler.ProfiledInterval ignored = profiler.profile() )
        {
            expensiveComputation( profiler );
        }
        otherIntenseWork();
        profiler.finish();
        String output = getProfilerOutput( profiler );
        assertThat( output ).contains( METHOD );
        assertThat( output ).doesNotContain( "otherIntensiveWork" );
    }

    private static String getProfilerOutput( Profiler profiler )
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try ( PrintStream out = new PrintStream( buffer ) )
        {
            profiler.printProfile( out, "Profile" );
            out.flush();
        }
        return buffer.toString();
    }

    private void expensiveComputation( Profiler profiler ) throws InterruptedException
    {
        do
        {
            Thread.sleep( COMPUTE_WORK_MILLIS );
        }
        while ( !getProfilerOutput( profiler ).contains( METHOD ) );
    }

    private static void otherIntenseWork() throws InterruptedException
    {
        Thread.sleep( COMPUTE_WORK_MILLIS );
    }
}
