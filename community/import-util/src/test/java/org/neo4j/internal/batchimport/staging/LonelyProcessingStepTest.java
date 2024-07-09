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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.batchimport.executor.ProcessorScheduler.SPAWN_THREAD;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;

class LonelyProcessingStepTest {
    @Test
    void issuePanicBeforeCompletionOnError() throws Exception {
        List<Step<?>> stepsPipeline = new ArrayList<>();
        TrackingPanicMonitor panicMonitor = new TrackingPanicMonitor();
        FaultyLonelyProcessingStepTest faultyStep = new FaultyLonelyProcessingStepTest(stepsPipeline, panicMonitor);
        stepsPipeline.add(faultyStep);

        faultyStep.receive(1, null);
        faultyStep.awaitCompleted();

        assertTrue(faultyStep.endOfUpstreamCalled);
        assertTrue(
                faultyStep.isPanicOnEndUpstream(),
                "On upstream end step should be already on panic in case of exception");
        assertTrue(faultyStep.isPanic());
        assertFalse(faultyStep.stillWorking());
        assertTrue(faultyStep.isCompleted());
        assertTrue(panicMonitor.hasReceivedPanic());
    }

    private static class FaultyLonelyProcessingStepTest extends LonelyProcessingStep {
        private volatile boolean endOfUpstreamCalled;
        private volatile boolean panicOnEndUpstream;

        FaultyLonelyProcessingStepTest(List<Step<?>> pipeLine, TrackingPanicMonitor panicMonitor) {
            super(
                    new StageExecution("Faulty", null, Configuration.DEFAULT, pipeLine, 0, SPAWN_THREAD, panicMonitor),
                    "Faulty",
                    Configuration.DEFAULT);
        }

        @Override
        protected void process() {
            throw new RuntimeException("Process exception");
        }

        @Override
        public void endOfUpstream() {
            endOfUpstreamCalled = true;
            panicOnEndUpstream = isPanic();
            super.endOfUpstream();
        }

        private boolean isPanicOnEndUpstream() {
            return panicOnEndUpstream;
        }
    }
}
