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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.batchimport.staging.ControlledStep.stepWithAverageOf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;

class StageExecutionTest {
    @Test
    void shouldOrderStepsAscending() {
        // GIVEN
        Collection<Step<?>> steps = new ArrayList<>();
        steps.add(stepWithAverageOf("step1", 0, 10));
        steps.add(stepWithAverageOf("step2", 0, 5));
        steps.add(stepWithAverageOf("step3", 0, 30));
        StageExecution execution =
                new StageExecution("Test", null, Configuration.DEFAULT, steps, Step.ORDER_SEND_DOWNSTREAM);

        // WHEN
        Iterator<WeightedStep> ordered =
                execution.stepsOrderedBy(Keys.avg_processing_time, true).iterator();

        // THEN
        WeightedStep fastest = ordered.next();
        assertEquals(1f / 2f, fastest.weight(), 0f);
        WeightedStep faster = ordered.next();
        assertEquals(1f / 3f, faster.weight(), 0f);
        WeightedStep fast = ordered.next();
        assertEquals(1f, fast.weight(), 0f);
        assertFalse(ordered.hasNext());
    }

    @Test
    void shouldOrderStepsDescending() {
        // GIVEN
        Collection<Step<?>> steps = new ArrayList<>();
        steps.add(stepWithAverageOf("step1", 0, 10));
        steps.add(stepWithAverageOf("step2", 0, 5));
        steps.add(stepWithAverageOf("step3", 0, 30));
        steps.add(stepWithAverageOf("step4", 0, 5));
        StageExecution execution =
                new StageExecution("Test", null, Configuration.DEFAULT, steps, Step.ORDER_SEND_DOWNSTREAM);

        // WHEN
        Iterator<WeightedStep> ordered =
                execution.stepsOrderedBy(Keys.avg_processing_time, false).iterator();

        // THEN
        WeightedStep slowest = ordered.next();
        assertEquals(3f, slowest.weight(), 0f);
        WeightedStep slower = ordered.next();
        assertEquals(2f, slower.weight(), 0f);
        WeightedStep slow = ordered.next();
        assertEquals(1f, slow.weight(), 0f);
        WeightedStep alsoSlow = ordered.next();
        assertEquals(1f, alsoSlow.weight(), 0f);
        assertFalse(ordered.hasNext());
    }
}
