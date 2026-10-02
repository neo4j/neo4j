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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.batchimport.staging.ControlledStep.stepWithStats;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;

class DynamicProcessorAssignerTest {
    @Test
    void shouldAssignProcessorsToSlowestStep() {
        // GIVEN
        Configuration config = config(10, 5);
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);

        ControlledStep<?> slowStep = stepWithStats("slow", 0, Keys.avg_processing_time, 10L, Keys.done_batches, 10L);
        ControlledStep<?> fastStep = stepWithStats("fast", 0, Keys.avg_processing_time, 2L, Keys.done_batches, 10L);

        StageExecution execution = executionOf(config, slowStep, fastStep);
        assigner.start(execution);

        // WHEN
        assigner.check(execution);

        // THEN
        assertThat(slowStep.processors(0)).isEqualTo(4);
        assertThat(fastStep.processors(0)).isOne();
    }

    @Test
    void shouldMoveProcessorFromOverlyAssignedStep() {
        // GIVEN
        Configuration config = config(10, 5);
        // available processors = 2 is enough because it will see the fast step as only using 20% of a processor
        // and it rounds down. So there's room for assigning one more.
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);

        ControlledStep<?> slowStep = stepWithStats("slow", 0, Keys.avg_processing_time, 6L, Keys.done_batches, 10L)
                .setProcessors(2);
        ControlledStep<?> fastStep = stepWithStats("fast", 0, Keys.avg_processing_time, 2L, Keys.done_batches, 10L)
                .setProcessors(3);

        StageExecution execution = executionOf(config, slowStep, fastStep);
        assigner.start(execution);

        // WHEN checking
        assigner.check(execution);

        // THEN one processor should have been moved from the fast step to the slower step
        assertThat(fastStep.processors(0)).isEqualTo(2);
        assertThat(slowStep.processors(0)).isEqualTo(3);
    }

    @Test
    void shouldNotMoveProcessorFromFastStepSoThatItBecomesBottleneck() {
        // GIVEN
        Configuration config = config(10, 4);
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);

        ControlledStep<?> slowStep = stepWithStats("slow", 1, Keys.avg_processing_time, 10L, Keys.done_batches, 10L);
        ControlledStep<?> fastStep = stepWithStats("fast", 0, Keys.avg_processing_time, 7L, Keys.done_batches, 10L)
                .setProcessors(3);

        StageExecution execution = executionOf(config, slowStep, fastStep);
        assigner.start(execution);

        // WHEN checking
        assigner.check(execution);

        // THEN
        assertThat(fastStep.processors(0)).isEqualTo(3);
        assertThat(slowStep.processors(0)).isOne();
    }

    @Test
    void shouldHandleZeroAverage() {
        // GIVEN
        Configuration config = config(10, 5);
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);

        ControlledStep<?> aStep = stepWithStats("slow", 0, Keys.avg_processing_time, 0L, Keys.done_batches, 0L);
        ControlledStep<?> anotherStep = stepWithStats("fast", 0, Keys.avg_processing_time, 0L, Keys.done_batches, 0L);

        StageExecution execution = executionOf(config, aStep, anotherStep);
        assigner.start(execution);

        // WHEN
        assigner.check(execution);

        // THEN
        assertThat(aStep.processors(0)).isOne();
        assertThat(anotherStep.processors(0)).isOne();
    }

    @Test
    void shouldMoveProcessorFromNotOnlyFastestStep() {
        // The point is that not only the fastest step is subject to have processors removed,
        // it's the relationship between all pairs of steps.

        // GIVEN
        Configuration config = config(10, 5);
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);
        Step<?> wayFastest = stepWithStats("wayFastest", 0, Keys.avg_processing_time, 50L, Keys.done_batches, 20L);
        Step<?> fast = stepWithStats("fast", 0, Keys.avg_processing_time, 100L, Keys.done_batches, 20L)
                .setProcessors(3);
        Step<?> slow = stepWithStats("slow", 2, Keys.avg_processing_time, 220L, Keys.done_batches, 20L);
        StageExecution execution = executionOf(config, slow, wayFastest, fast);
        assigner.start(execution);

        // WHEN
        assigner.check(execution);

        // THEN
        assertThat(fast.processors(0)).isEqualTo(2);
        assertThat(slow.processors(0)).isEqualTo(2);
    }

    @Test
    void shouldNotMoveProcessorFromOverlyAssignedStepIfRemainingPermits() {
        // GIVEN
        Configuration config = config(10, 10);
        DynamicProcessorAssigner assigner = new DynamicProcessorAssigner(config);
        Step<?> wayFastest = stepWithStats("wayFastest", 0, Keys.avg_processing_time, 50L, Keys.done_batches, 20L);
        Step<?> fast = stepWithStats("fast", 0, Keys.avg_processing_time, 100L, Keys.done_batches, 20L)
                .setProcessors(3);
        Step<?> slow = stepWithStats("slow", 2, Keys.avg_processing_time, 220L, Keys.done_batches, 20L);
        StageExecution execution = executionOf(config, slow, wayFastest, fast);
        assigner.start(execution);

        // WHEN
        assigner.check(execution);

        // THEN no processor have been removed from the fast step
        assertThat(fast.processors(0)).isEqualTo(3);
        // although there were some to assign so slow step got one
        assertThat(slow.processors(0)).isEqualTo(2);
    }

    private static Configuration config(final int movingAverage, int processors) {
        return new Configuration.Overridden(Configuration.DEFAULT) {
            @Override
            public int movingAverageSize() {
                return movingAverage;
            }

            @Override
            public int maxNumberOfWorkerThreads() {
                return processors;
            }
        };
    }

    private static StageExecution executionOf(Configuration config, Step<?>... steps) {
        return new StageExecution("Test", null, config, Arrays.asList(steps), Step.ORDER_SEND_DOWNSTREAM);
    }
}
