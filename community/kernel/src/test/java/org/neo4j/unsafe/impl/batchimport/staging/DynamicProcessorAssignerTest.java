/*
 * Copyright (c) "Neo4j"
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
import static org.neo4j.unsafe.impl.batchimport.staging.ControlledStep.stepWithStats;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.unsafe.impl.batchimport.stats.Keys.avg_processing_time;
import static org.neo4j.unsafe.impl.batchimport.stats.Keys.done_batches;

public class DynamicProcessorAssignerTest
{
    @Test
    public void shouldAssignProcessorsToSlowestStep()
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
        assertEquals( 4, slowStep.processors( 0 ) );
        assertEquals( 1, fastStep.processors( 0 ) );
    }

    @Test
    public void shouldMoveProcessorFromOverlyAssignedStep()
    {
        // GIVEN
        Configuration config = config( 10, 5 );
        // available processors = 2 is enough because it will see the fast step as only using 20% of a processor
        // and it rounds down. So there's room for assigning one more.
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> slowStep = stepWithStats( "slow", 0, avg_processing_time, 6L, done_batches, 10L ).setProcessors( 2 );
        ControlledStep<?> fastStep = stepWithStats( "fast", 0, avg_processing_time, 2L, done_batches, 10L ).setProcessors( 3 );

        StageExecution execution = executionOf( config, slowStep, fastStep );
        assigner.start( execution );

        // WHEN checking
        assigner.check( execution );

        // THEN one processor should have been moved from the fast step to the slower step
        assertEquals( 2, fastStep.processors( 0 ) );
        assertEquals( 3, slowStep.processors( 0 ) );
    }

    @Test
    public void shouldNotMoveProcessorFromFastStepSoThatItBecomesBottleneck()
    {
        // GIVEN
        Configuration config = config( 10, 4 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );

        ControlledStep<?> slowStep = stepWithStats( "slow", 1, avg_processing_time, 10L, done_batches, 10L );
        ControlledStep<?> fastStep = stepWithStats( "fast", 0, avg_processing_time, 7L, done_batches, 10L ).setProcessors( 3 );

        StageExecution execution = executionOf( config, slowStep, fastStep );
        assigner.start( execution );

        // WHEN checking
        assigner.check( execution );

        // THEN
        assertEquals( 3, fastStep.processors( 0 ) );
        assertEquals( 1, slowStep.processors( 0 ) );
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
    public void shouldMoveProcessorFromNotOnlyFastestStep()
    {
        // The point is that not only the fastest step is subject to have processors removed,
        // it's the relationship between all pairs of steps.

        // GIVEN
        Configuration config = config( 10, 5 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );
        Step<?> wayFastest = stepWithStats( "wayFastest", 0, avg_processing_time, 50L, done_batches, 20L );
        Step<?> fast = stepWithStats( "fast", 0, avg_processing_time, 100L, done_batches, 20L ).setProcessors( 3 );
        Step<?> slow = stepWithStats( "slow", 2, avg_processing_time, 220L, done_batches, 20L );
        StageExecution execution = executionOf( config, slow, wayFastest, fast );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN
        assertEquals( 2, fast.processors( 0 ) );
        assertEquals( 2, slow.processors( 0 ) );
    }

    @Test
    public void shouldNotMoveProcessorFromOverlyAssignedStepIfRemainingPermits()
    {
        // GIVEN
        Configuration config = config( 10, 10 );
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner( config );
        Step<?> wayFastest = stepWithStats( "wayFastest", 0, avg_processing_time, 50L, done_batches, 20L );
        Step<?> fast = stepWithStats( "fast", 0, avg_processing_time, 100L, done_batches, 20L ).setProcessors( 3 );
        Step<?> slow = stepWithStats( "slow", 2, avg_processing_time, 220L, done_batches, 20L );
        StageExecution execution = executionOf( config, slow, wayFastest, fast );
        assigner.start( execution );

        // WHEN
        assigner.check( execution );

        // THEN no processor have been removed from the fast step
        assertEquals( 3, fast.processors( 0 ) );
        // although there were some to assign so slow step got one
        assertEquals( 2, slow.processors( 0 ) );
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
