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
package org.neo4j.resources;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

class ProfilerTest
{
    private static final int COMPUTE_WORK_MILLIS = 1000;

    @Test
    void profilerMustNoticeWhereTimeGoes() throws Exception
    {
        Profiler profiler = Profiler.profiler();
        try ( Profiler.ProfiledInterval ignored = profiler.profile() )
        {
            expensiveComputation();
        }
        String output = getProfilerOutput( profiler );
        assertThat( output, containsString( "expensiveComputation" ) );
    }

    @Test
    void profilerMustLimitItselfToProfiledRegion() throws Exception
    {
        Profiler profiler = Profiler.profiler();
        try ( Profiler.ProfiledInterval ignored = profiler.profile() )
        {
            expensiveComputation();
        }
        otherIntenseWork();
        String output = getProfilerOutput( profiler );
        assertThat( output, not( containsString( "otherIntensiveWork" ) ) );
    }

    private static String getProfilerOutput( Profiler profiler ) throws InterruptedException
    {
        profiler.finish();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try ( PrintStream out = new PrintStream( buffer ) )
        {
            profiler.printProfile( out, "Profile" );
            out.flush();
        }
        return buffer.toString();
    }

    private static void expensiveComputation() throws InterruptedException
    {
        Thread.sleep( COMPUTE_WORK_MILLIS );
    }

    private static void otherIntenseWork() throws InterruptedException
    {
        Thread.sleep( COMPUTE_WORK_MILLIS );
    }
}
