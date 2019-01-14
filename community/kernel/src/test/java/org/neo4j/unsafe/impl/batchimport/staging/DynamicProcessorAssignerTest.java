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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.unsafe.impl.batchimport.staging.ControlledStep.stepWithStats;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.unsafe.impl.batchimport.stats.Keys.avg_processing_time;
import static org.neo4j.unsafe.impl.batchimport.stats.Keys.done_batches;

public class DynamicProcessorAssignerTest
{
    @Test
    public void shouldAssignAdditionalCPUToBottleNeckStep()
    {
        // GIVEN
        Configuration config = config( 10, 5 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> slowStep = stepWithStats( "slow", 0, avg_processing_time, 10L, done_batches, 10L );
        ControlledStep<?> fastStep = stepWithStats( "fast", 0, avg_processing_time, 2L, done_batches, 10L );

        StageExecution execution = executionOf( config, slowStep, fastStep );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN
        assertEquals( 5, slowStep.processors( 0 ) );
        assertEquals( 1, fastStep.processors( 0 ) );
    }

    @Test
    public void shouldRemoveCPUsFromWayTooFastStep()
    {
        // GIVEN
        Configuration config = config( 10, 3 );
        // available processors = 2 is enough because it will see the fast step as only using 20% of a processor
        // and it rounds down. So there's room for assigning one more.
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> slowStep = spy( stepWithStats( "slow", 1, avg_processing_time, 6L, done_batches, 10L )
                .setProcessors( 2 ) );
        ControlledStep<?> fastStep = spy( stepWithStats( "fast", 0, avg_processing_time, 2L, done_batches, 10L )
                .setProcessors( 2 ) );

        StageExecution execution = executionOf( config, slowStep, fastStep );
        assigner.start( execution );

        // WHEN checking
        assigner.check( execution );

        // THEN one processor should be removed from the fast step
        verify( fastStep, times( 1 ) ).processors( -1 );
    }

    @Test
    public void shouldRemoveCPUsButNotSoThatTheFastStepBecomesBottleneck()
    {
        // GIVEN
        Configuration config = config( 10, 3 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> slowStep = spy( stepWithStats( "slow", 1, avg_processing_time, 10L, done_batches, 10L ) );
        ControlledStep<?> fastStep = spy( stepWithStats( "fast", 0, avg_processing_time, 7L, done_batches, 10L )
                .setProcessors( 3 ) );

        StageExecution execution = executionOf( config, slowStep, fastStep );
        assigner.start( execution );

        // WHEN checking the first time
        assigner.check( execution );

        // THEN one processor should be removed from the fast step
        verify( fastStep, never() ).processors( 1 );
        verify( fastStep, never() ).processors( -1 );
    }

    @Test
    public void shouldHandleZeroAverage()
    {
        // GIVEN
        Configuration config = config( 10, 5 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> aStep = stepWithStats( "slow", 0, avg_processing_time, 0L, done_batches, 0L );
        ControlledStep<?> anotherStep = stepWithStats( "fast", 0, avg_processing_time, 0L, done_batches, 0L );

        StageExecution execution = executionOf( config, aStep, anotherStep );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN
        assertEquals( 1, aStep.processors( 0 ) );
        assertEquals( 1, anotherStep.processors( 0 ) );
    }

    @Test
    public void shouldRemoveCPUsFromTooFastStepEvenIfThereIsAWayFaster()
    {
        // The point is that not only the fastest step is subject to have processors removed,
        // it's the relationship between all pairs of steps. This is important since the DPA has got
        // a max permit count of processors to assign, so reclaiming unnecessary assignments can
        // have those be assigned to a more appropriate step instead, where it will benefit the Stage more.

        // GIVEN
        Configuration config = config( 10, 3 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );
        Step<?> wayFastest = stepWithStats( "wayFastest", 0, avg_processing_time, 50L, done_batches, 20L );
        Step<?> fast = spy( stepWithStats( "fast", 0, avg_processing_time, 100L, done_batches, 20L )
                .setProcessors( 3 ) );
        Step<?> slow = stepWithStats( "slow", 1, avg_processing_time, 220L, done_batches, 20L );
        StageExecution execution = executionOf( config, slow, wayFastest, fast );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN
        verify( fast ).processors( -1 );
    }

    @Test
    public void shouldRemoveCPUsFromTooFastStepEvenIfNotAllPermitsAreUsed()
    {
        // GIVEN
        Configuration config = config( 10, 20 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );
        Step<?> wayFastest = spy( stepWithStats( "wayFastest", 0,
                avg_processing_time, 50L,
                done_batches, 20L )
                .setProcessors( 5 ) );
        Step<?> fast = spy( stepWithStats( "fast", 0,
                avg_processing_time, 100L,
                done_batches, 20L )
                .setProcessors( 3 ) );
        Step<?> slow = stepWithStats( "slow", 1,
                avg_processing_time, 220L,
                done_batches, 20L );
        StageExecution execution = executionOf( config, slow, wayFastest, fast );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN
        verify( wayFastest ).processors( -1 );
    }

    private Configuration config( final int movingAverage, int processors )
    {
        return new Configuration()
        {
            @Override
            public int movingAverageSize()
            {
                return movingAverage;
            }

            @Override
            public int maxNumberOfProcessors()
            {
                return processors;
            }
        };
    }

    private StageExecution executionOf( Configuration config, Step<?>... steps )
    {
        return new StageExecution( "Test", null, config, Arrays.asList( steps ), ORDER_SEND_DOWNSTREAM );
    }
}
