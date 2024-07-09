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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;

class CoarseBoundedProgressExecutionMonitorTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 10, 123})
    void shouldReportProgressOnSingleExecution(int batchSize) {
        // GIVEN
        Configuration config = config(batchSize);
        ProgressExecutionMonitor progressExecutionMonitor = new ProgressExecutionMonitor(batchSize, config(batchSize));

        // WHEN
        long total = monitorSingleStageExecution(progressExecutionMonitor, config);

        // THEN
        assertEquals(total, progressExecutionMonitor.getProgress());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 123})
    void progressOnMultipleExecutions(int batchSize) {
        Configuration config = config(batchSize);
        ProgressExecutionMonitor progressExecutionMonitor = new ProgressExecutionMonitor(batchSize, config);

        long total = progressExecutionMonitor.total();

        for (int i = 0; i < 4; i++) {
            progressExecutionMonitor.start(execution(0, config));
            progressExecutionMonitor.check(execution(total / 4, config));
        }
        progressExecutionMonitor.done(true, 0, "Completed");

        assertEquals(total, progressExecutionMonitor.getProgress(), "Each item should be completed");
    }

    private static long monitorSingleStageExecution(
            ProgressExecutionMonitor progressExecutionMonitor, Configuration config) {
        progressExecutionMonitor.start(execution(0, config));
        long total = progressExecutionMonitor.total();
        long part = total / 10;
        for (int i = 0; i < 9; i++) {
            progressExecutionMonitor.check(execution(part * (i + 1), config));
            assertTrue(progressExecutionMonitor.getProgress() < total);
        }
        progressExecutionMonitor.done(true, 0, "Test");
        return total;
    }

    private static StageExecution execution(long doneBatches, Configuration config) {
        Step<?> step = ControlledStep.stepWithStats("Test", 0, Keys.done_batches, doneBatches);
        return new StageExecution("Test", null, config, Collections.singletonList(step), 0);
    }

    private static Configuration config(int batchSize) {
        return new Configuration.Overridden(Configuration.DEFAULT) {
            @Override
            public int batchSize() {
                return batchSize;
            }
        };
    }

    private static class ProgressExecutionMonitor extends CoarseBoundedProgressExecutionMonitor {
        private long progress;

        ProgressExecutionMonitor(int batchSize, Configuration configuration) {
            super(100 * batchSize, 100 * batchSize, configuration);
        }

        @Override
        protected void progress(long progress) {
            this.progress += progress;
        }

        long getProgress() {
            return progress;
        }
    }
}
