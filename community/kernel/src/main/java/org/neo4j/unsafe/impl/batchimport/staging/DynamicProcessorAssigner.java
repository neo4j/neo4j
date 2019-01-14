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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static java.lang.Integer.min;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Monitors {@link StageExecution executions} and makes changes as the execution goes:
 * <ul>
 * <li>Figures out roughly how many CPUs (henceforth called processors) are busy processing batches.
 * The most busy step will have its {@link Step#processors(int) processors} counted as 1 processor each, all other
 * will take into consideration how idle the CPUs executing each step is, counted as less than one.</li>
 * <li>Constantly figures out bottleneck steps and assigns more processors those.</li>
 * <li>Constantly figures out if there are steps that are way faster than the second fastest step and
 * removes processors from those steps.</li>
 * <li>At all times keeps the total number of processors assigned to steps to a total of less than or equal to
 * {@link Configuration#maxNumberOfProcessors()}.</li>
 * </ul>
 */
public class DynamicProcessorAssigner extends ExecutionMonitor.Adapter
{
    private final Configuration config;
    private final Map<Step<?>,Long/*done batches*/> lastChangedProcessors = new HashMap<>();
    private final int availableProcessors;

    public DynamicProcessorAssigner( Configuration config )
    {
        super( 1, SECONDS );
        this.config = config;
        this.availableProcessors = config.maxNumberOfProcessors();
    }

    @Override
    public void start( StageExecution execution )
    {   // A new stage begins, any data that we had is irrelevant
        lastChangedProcessors.clear();
    }

    @Override
    public void check( StageExecution execution )
    {
        if ( execution.stillExecuting() )
        {
            int permits = availableProcessors - countActiveProcessors( execution );
            if ( permits > 0 )
            {
                // Be swift at assigning processors to slow steps, i.e. potentially multiple per round
                assignProcessorsToPotentialBottleNeck( execution, permits );
            }
            // Be a little more conservative removing processors from too fast steps
            removeProcessorFromPotentialIdleStep( execution );
        }
    }

    private void assignProcessorsToPotentialBottleNeck( StageExecution execution, int permits )
    {
        Pair<Step<?>,Float> bottleNeck = execution.stepsOrderedBy( Keys.avg_processing_time, false ).iterator().next();
        Step<?> bottleNeckStep = bottleNeck.first();
        long doneBatches = batches( bottleNeckStep );
        if ( bottleNeck.other() > 1.0f &&
             batchesPassedSinceLastChange( bottleNeckStep, doneBatches ) >= config.movingAverageSize() )
        {
            // Assign 1/10th of the remaining permits. This will have processors being assigned more
            // aggressively in the beginning of the run
            int optimalProcessorIncrement = min( max( 1, (int) bottleNeck.other().floatValue() - 1 ), permits );
            int before = bottleNeckStep.processors( 0 );
            int after = bottleNeckStep.processors( max( optimalProcessorIncrement, permits / 10 ) );
            if ( after > before )
            {
                lastChangedProcessors.put( bottleNeckStep, doneBatches );
            }
        }
    }

    private void removeProcessorFromPotentialIdleStep( StageExecution execution )
    {
        for ( Pair<Step<?>,Float> fast : execution.stepsOrderedBy( Keys.avg_processing_time, true ) )
        {
            int numberOfProcessors = fast.first().processors( 0 );
            if ( numberOfProcessors == 1 )
            {
                continue;
            }

            // Translate the factor compared to the next (slower) step and see if this step would still
            // be faster if we decremented the processor count, with a slight conservative margin as well
            // (0.8 instead of 1.0 so that we don't decrement and immediately become the bottleneck ourselves).
            float factorWithDecrementedProcessorCount =
                    fast.other() * numberOfProcessors / (numberOfProcessors - 1);
            if ( factorWithDecrementedProcessorCount < 0.8f )
            {
                Step<?> fastestStep = fast.first();
                long doneBatches = batches( fastestStep );
                if ( batchesPassedSinceLastChange( fastestStep, doneBatches ) >= config.movingAverageSize() )
                {
                    int before = fastestStep.processors( 0 );
                    if ( fastestStep.processors( -1 ) < before )
                    {
                        lastChangedProcessors.put( fastestStep, doneBatches );
                        return;
                    }
                }
            }
        }
    }

    private long avg( Step<?> step )
    {
        return step.stats().stat( Keys.avg_processing_time ).asLong();
    }

    private long batches( Step<?> step )
    {
        return step.stats().stat( Keys.done_batches ).asLong();
    }

    private int countActiveProcessors( StageExecution execution )
    {
        float processors = 0;
        if ( execution.stillExecuting() )
        {
            long highestAverage = avg( execution.stepsOrderedBy(
                    Keys.avg_processing_time, false ).iterator().next().first() );
            for ( Step<?> step : execution.steps() )
            {
                // Calculate how active each step is so that a step that is very cheap
                // and idles a lot counts for less than 1 processor, so that bottlenecks can
                // "steal" some of its processing power.
                long avg = avg( step );
                float factor = (float)avg / (float)highestAverage;
                processors += factor * step.processors( 0 );
            }
        }
        return Math.round( processors );
    }

    private long batchesPassedSinceLastChange( Step<?> step, long doneBatches )
    {
        return lastChangedProcessors.containsKey( step )
                // <doneBatches> number of batches have passed since the last change to this step
                ? doneBatches - lastChangedProcessors.get( step )
                // we have made no changes to this step yet, go ahead
                : config.movingAverageSize();
    }
}
