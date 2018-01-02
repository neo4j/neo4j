/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;

import static java.lang.String.format;

/**
 * Processor assigner strategies that are useful for testing {@link ParallelBatchImporter} as to exercise
 * certain aspects of parallelism which would otherwise only be exercised on particular machines and datasets.
 */
public class ProcessorAssignmentStrategies
{
    /**
     * Right of the bat assigns all permitted processors to random steps that allow multiple threads.
     */
    public static ExecutionMonitor eagerRandomSaturation( final int availableProcessor )
    {
        return new AbstractAssigner( Clock.SYSTEM_CLOCK, 10, TimeUnit.SECONDS )
        {
            @Override
            public void start( StageExecution[] executions )
            {
                saturate( availableProcessor, executions );
                registerProcessorCount( executions );
            }

            private void saturate( final int availableProcessor, StageExecution[] executions )
            {
                Random random = ThreadLocalRandom.current();
                int processors = availableProcessor;
                for ( int rounds = 0; rounds < availableProcessor && processors > 0; rounds++ )
                {
                    for ( StageExecution execution : executions )
                    {
                        for ( Step<?> step : execution.steps() )
                        {
                            if ( random.nextBoolean() && step.incrementNumberOfProcessors() && --processors == 0 )
                            {
                                return;
                            }
                        }
                    }
                }
            }

            @Override
            public void check( StageExecution[] executions )
            {   // We do everything in start
            }
        };
    }

    /**
     * For every check assigns a random number of more processors to random steps that allow multiple threads.
     */
    public static ExecutionMonitor randomSaturationOverTime( final int availableProcessor )
    {
        return new AbstractAssigner( Clock.SYSTEM_CLOCK, 100, TimeUnit.MILLISECONDS )
        {
            private int processors = availableProcessor;

            @Override
            public void check( StageExecution[] executions )
            {
                saturate( executions );
                registerProcessorCount( executions );
            }

            private void saturate( StageExecution[] executions )
            {
                if ( processors == 0 )
                {
                    return;
                }

                Random random = ThreadLocalRandom.current();
                int maxThisCheck = random.nextInt( processors-1 )+1;
                for ( StageExecution execution : executions )
                {
                    for ( Step<?> step : execution.steps() )
                    {
                        if ( random.nextBoolean() && step.incrementNumberOfProcessors() )
                        {
                            processors--;
                            if ( --maxThisCheck == 0 )
                            {
                                return;
                            }
                        }
                    }
                }
            }
        };
    }

    private static abstract class AbstractAssigner extends ExecutionMonitor.Adapter
    {
        private final Map<String,Map<String,Integer>> processors = new HashMap<>();

        protected AbstractAssigner( Clock clock, long time, TimeUnit unit )
        {
            super( clock, time, unit );
        }

        protected void registerProcessorCount( StageExecution[] executions )
        {
            for ( StageExecution execution : executions )
            {
                Map<String,Integer> byStage = new HashMap<>();
                processors.put( execution.getStageName(), byStage );
                for ( Step<?> step : execution.steps() )
                {
                    byStage.put( step.name(), step.numberOfProcessors() );
                }
            }
        }

        @Override
        public String toString()
        {
            // For debugging purposes. Includes information about how many processors each step got.
            StringBuilder builder = new StringBuilder();
            for ( String stage : processors.keySet() )
            {
                builder.append( stage ).append( ':' );
                Map<String,Integer> byStage = processors.get( stage );
                for ( String step : byStage.keySet() )
                {
                    builder.append( format( "%n  %s:%d", step, byStage.get( step ) ) );
                }
                builder.append( format( "%n" ) );
            }
            return builder.toString();
        }
    }
}
