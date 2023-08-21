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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MultiExecutionMonitorTest {
    @Test
    void shouldCheckMultipleMonitors() {
        // GIVEN
        TestableMonitor first = new TestableMonitor(100, MILLISECONDS, "first");
        TestableMonitor other = new TestableMonitor(250, MILLISECONDS, "other");
        MultiExecutionMonitor multiMonitor = new MultiExecutionMonitor(first, other);

        // WHEN/THEN
        assertThat(multiMonitor.checkIntervalMillis()).isEqualTo(100L);
        expectCallsToCheck(multiMonitor, first, 1, other, 0);
        expectCallsToCheck(multiMonitor, first, 2, other, 0);
        expectCallsToCheck(multiMonitor, first, 3, other, 1);
    }

    private static void expectCallsToCheck(ExecutionMonitor multiMonitor, Object... alternatingMonitorAndCount) {
        multiMonitor.check(null); // null, knowing that our monitors in this test doesn't use 'em
        for (int i = 0; i < alternatingMonitorAndCount.length; i++) {
            TestableMonitor monitor = (TestableMonitor) alternatingMonitorAndCount[i++];
            int count = (Integer) alternatingMonitorAndCount[i];
            assertThat(monitor.timesPolled).isLessThanOrEqualTo(count);
            if (monitor.timesPolled < count) {
                fail("Polls didn't occur, expected " + Arrays.toString(alternatingMonitorAndCount));
            }
        }
    }

    private static class TestableMonitor extends ExecutionMonitor.Adapter {
        private int timesPolled;
        private final String name;

        TestableMonitor(long interval, TimeUnit unit, String name) {
            super(interval, unit);
            this.name = name;
        }

        @Override
        public void check(StageExecution execution) {
            timesPolled++;
        }

        @Override
        public String toString() {
            return "[" + name + ":" + timesPolled + "]";
        }
    }
}
