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
package org.neo4j.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.ToolingMemoryCalculations.NO_MONITOR;
import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.neo4j.io.os.OsBeanUtil;

class ToolingMemoryCalculationsTest {
    @Test
    void shouldCalculateCorrectMaxMemorySetting() {
        // given
        long freeMachineMemory = OsBeanUtil.getFreePhysicalMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        assumeTrue(freeMachineMemory != VALUE_UNAVAILABLE);
        int percent = 70;

        // when
        long memory = new ToolingMemoryCalculations(NO_MONITOR)
                .calculateMaxAvailableOffHeapMemoryFromPercent(percent, freeMachineMemory, maxMemory);

        // then
        long expected = Math.round((freeMachineMemory - Math.min(maxMemory, freeMachineMemory / 2)) * (percent / 100D));
        assertThat(memory)
                .as("Machine free memory: " + freeMachineMemory + ", max: " + maxMemory)
                .isEqualTo(expected);
    }
}
