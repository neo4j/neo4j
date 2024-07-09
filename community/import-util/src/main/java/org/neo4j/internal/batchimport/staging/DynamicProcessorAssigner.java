/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.staging;

import static java.lang.Integer.min;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.batchimport.stats.Keys.done_batches;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;

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
 * {@link Configuration#maxNumberOfWorkerThreads()}.</li>
 * </ul>
 */
public class DynamicProcessorAssigner extends ExecutionMonitor.Adapter {
    private final Configuration config;
    private final Map<Step<?>, Long /*done batches*/> lastChangedProcessors = new HashMap<>();
    private final int availableProcessors;

    public DynamicProcessorAssigner(Configuration config) {
        super(1, SECONDS);
        this.config = config;
        this.availableProcessors = config.maxNumberOfWorkerThreads();
    }

    @Override
    public void start(StageExecution execution) { // A new stage begins, any data that we had is irrelevant
        lastChangedProcessors.clear();
    }

    @Override
    public void check(StageExecution execution) {
        if (execution.stillExecuting()) {
            int permits = availableProcessors - countActiveProcessors(execution);
            if (permits > 0) {
                // Be swift at assigning processors to slow steps, i.e. potentially multiple per round
                permits -= assignProcessors(execution, permits);
            }
            // Be a little more conservative removing processors from too fast steps
            if (permits == 0) {
                moveProcessorFromOverlyAssigned(execution);
            }
        }
    }

    private int assignProcessors(StageExecution execution, int permits) {
        WeightedStep bottleNeck = execution
                .stepsOrderedBy(Keys.avg_processing_time, false)
                .iterator()
                .next();
        Step<?> bottleNeckStep = bottleNeck.step();
        long doneBatches = bottleNeckStep.longStat(done_batches);
        if (bottleNeck.weight() > 1.0f
                && batchesPassedSinceLastChange(bottleNeckStep, doneBatches) >= config.movingAverageSize()) {
            // Assign 1/10th of the remaining permits. This will have processors being assigned more
            // aggressively in the beginning of the run
            int optimalProcessorIncrement = min(max(1, (int) bottleNeck.weight().floatValue() - 1), permits);
            int before = bottleNeckStep.processors(0);
            int after = bottleNeckStep.processors(max(optimalProcessorIncrement, permits / 10));
            if (after > before) {
                lastChangedProcessors.put(bottleNeckStep, doneBatches);
            }
            return after - before;
        }
        return 0;
    }

    private void moveProcessorFromOverlyAssigned(StageExecution execution) {
        List<WeightedStep> steps = execution.stepsOrderedBy(Keys.avg_processing_time, true);
        for (int i = 0; i < steps.size() - 1; i++) {
            WeightedStep faster = steps.get(i);
            Step<?> fasterStep = faster.step();
            WeightedStep slower = steps.get(i + 1);
            Step<?> slowerStep = slower.step();
            int numberOfProcessors = faster.step().processors(0);
            if (numberOfProcessors == 1 || slowerStep.processors(0) == slowerStep.maxProcessors()) {
                continue;
            }

            // Translate the factor compared to the next (slower) step and see if this step would still
            // be faster if we decremented the processor count, with a slight conservative margin as well
            // (0.8 instead of 1.0 so that we don't decrement and immediately become the bottleneck ourselves).
            float factorWithDecrementedProcessorCount = faster.weight() * numberOfProcessors / (numberOfProcessors - 1);
            if (factorWithDecrementedProcessorCount < 0.8f) {
                long doneBatches = fasterStep.longStat(done_batches);
                if (batchesPassedSinceLastChange(fasterStep, doneBatches) >= config.movingAverageSize()) {
                    if (fasterStep.processors(-1) < numberOfProcessors) {
                        // OK, we pulled one from the faster step which had unnecessarily many processors.
                        lastChangedProcessors.put(fasterStep, doneBatches);
                        // Now give that one to the slower step
                        slowerStep.processors(1);
                        lastChangedProcessors.put(slowerStep, doneBatches);
                        return;
                    }
                }
            }
        }
    }

    private static int countActiveProcessors(StageExecution execution) {
        return execution.stillExecuting()
                ? StreamSupport.stream(execution.steps().spliterator(), false)
                        .mapToInt(step -> step.processors(0))
                        .sum()
                : 0;
    }

    private long batchesPassedSinceLastChange(Step<?> step, long doneBatches) {
        return lastChangedProcessors.containsKey(step)
                // <doneBatches> number of batches have passed since the last change to this step
                ? doneBatches - lastChangedProcessors.get(step)
                // we have made no changes to this step yet, go ahead
                : config.movingAverageSize();
    }
}
