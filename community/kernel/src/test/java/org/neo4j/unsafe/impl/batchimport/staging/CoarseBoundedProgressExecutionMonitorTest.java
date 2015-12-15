/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.ArrayUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static java.util.Arrays.asList;

import static org.neo4j.unsafe.impl.batchimport.stats.Keys.done_batches;

@RunWith( Parameterized.class )
public class CoarseBoundedProgressExecutionMonitorTest
{
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> parameters()
    {
        List<Object[]> result = new ArrayList<>();
        result.add( ArrayUtil.<Object>array( 1 ) );
        result.add( ArrayUtil.<Object>array( 10 ) );
        result.add( ArrayUtil.<Object>array( 123 ) );
        return result;
    }

    @Parameter( 0 )
    public int batchSize;

    @Test
    public void shouldReportProgressOnSingleExecution() throws Exception
    {
        // GIVEN
        final AtomicLong progress = new AtomicLong();
        Configuration config = config();
        CoarseBoundedProgressExecutionMonitor monitor = new CoarseBoundedProgressExecutionMonitor(
                100 * batchSize, 100 * batchSize, config )
        {
            @Override
            protected void progress( long add )
            {
                progress.addAndGet( add );
            }
        };

        // WHEN
        monitor.start( singleExecution( 0, config ) );
        long total = monitor.total();
        long part = total / 10;
        for ( int i = 0; i < 9; i++ )
        {
            monitor.check( singleExecution( part * (i+1), config ) );
            assertTrue( progress.get() < total );
        }
        monitor.done( 0, "Test" );

        // THEN
        assertEquals( total, progress.get() );
    }

    private StageExecution[] singleExecution( long doneBatches, Configuration config )
    {
        Step<?> step = ControlledStep.stepWithStats( "Test", 0, done_batches, doneBatches );
        StageExecution execution = new StageExecution( "Test", config, asList( step ), 0 );
        return new StageExecution[] {execution};
    }

    private Configuration config()
    {
        return new Configuration.Overridden( Configuration.DEFAULT )
        {
            @Override
            public int batchSize()
            {
                return batchSize;
            }
        };
    }
}
