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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.unsafe.impl.batchimport.stats.Keys.done_batches;

@RunWith( Parameterized.class )
public class CoarseBoundedProgressExecutionMonitorTest
{
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Integer> parameters()
    {
        return Arrays.asList(1, 10, 123);
    }

    @Parameter
    public int batchSize;

    @Test
    public void shouldReportProgressOnSingleExecution() throws Exception
    {
        // GIVEN
        Configuration config = config();
        ProgressExecutionMonitor progressExecutionMonitor = new ProgressExecutionMonitor(batchSize, config());

        // WHEN
        long total = monitorSingleStageExecution( progressExecutionMonitor, config );

        // THEN
        assertEquals( total, progressExecutionMonitor.getProgress() );
    }

    @Test
    public void progressOnMultipleExecutions()
    {
        Configuration config = config();
        ProgressExecutionMonitor progressExecutionMonitor = new ProgressExecutionMonitor(batchSize, config );

        long total = progressExecutionMonitor.total();

        for ( int i = 0; i < 4; i++ )
        {
            progressExecutionMonitor.start( singleExecution( 0, config ) );
            progressExecutionMonitor.check( singleExecution( total / 4, config ) );
        }
        progressExecutionMonitor.done( 0, "Completed" );

        assertEquals( "Each item should be completed", total, progressExecutionMonitor.getProgress());
    }

    private long monitorSingleStageExecution( ProgressExecutionMonitor progressExecutionMonitor, Configuration config )
    {
        progressExecutionMonitor.start( singleExecution( 0, config ) );
        long total = progressExecutionMonitor.total();
        long part = total / 10;
        for ( int i = 0; i < 9; i++ )
        {
            progressExecutionMonitor.check( singleExecution( part * (i+1), config ) );
            assertTrue( progressExecutionMonitor.getProgress() < total );
        }
        progressExecutionMonitor.done( 0, "Test" );
        return total;
    }

    private StageExecution[] singleExecution( long doneBatches, Configuration config )
    {
        Step<?> step = ControlledStep.stepWithStats( "Test", 0, done_batches, doneBatches );
        StageExecution execution = new StageExecution( "Test", config, Collections.singletonList( step ), 0 );
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

    private class ProgressExecutionMonitor extends CoarseBoundedProgressExecutionMonitor
    {
        private long progress = 0;

        ProgressExecutionMonitor( int batchSize, Configuration configuration )
        {
            super( 100 * batchSize, 100 * batchSize, configuration );
        }

        @Override
        protected void progress( long progress )
        {
            this.progress += progress;
        }

        public long getProgress()
        {
            return progress;
        }
    }
}
