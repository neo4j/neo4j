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

import static java.lang.Math.round;
import static org.neo4j.configuration.SettingValueParsers.parseLongWithUnit;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.util.Preconditions.requireBetween;

import org.neo4j.io.os.OsBeanUtil;

public class ToolingMemoryCalculations {
    public static final Monitor NOTIFY_SYS_ERR = amountOfMemoryUsedInstead ->
            System.err.println("WARNING: amount of free memory couldn't be detected so defaults to "
                    + bytesToString(amountOfMemoryUsedInstead)
                    + ". For optimal performance instead explicitly specify amount of "
                    + "memory using --max-off-heap-memory");

    public static final Monitor NO_MONITOR = amountOfMemoryUsedInstead -> {};

    private final Monitor monitor;

    /**
     * @param monitor for getting notified when e.g. machine memory couldn't be detected.
     */
    public ToolingMemoryCalculations(Monitor monitor) {
        this.monitor = monitor;
    }

    /**
     * Calculates max amount of available off-heap memory, for e.g. a tool to use. The returned value
     * looks at available memory on the host machine, amount of memory used by the JVM and also caters
     * for that there should be some amount of memory left for the OS, to avoid most situations where the
     * OS may decide to kill processes due to running out of memory.
     * @param percent percent of available memory to use.
     * @return the amount of off-heap memory available for allocation, based on the given {@code percent}.
     */
    public long calculateMaxAvailableOffHeapMemoryFromPercent(int percent) {
        return calculateMaxAvailableOffHeapMemoryFromPercent(
                percent,
                OsBeanUtil.getFreePhysicalMemory(),
                Runtime.getRuntime().maxMemory());
    }

    /**
     * Calculates max amount of available off-heap memory, for e.g. a tool to use. The returned value
     * looks at available memory on the host machine, amount of memory used by the JVM and also caters
     * for that there should be some amount of memory left for the OS, to avoid most situations where the
     * OS may decide to kill processes due to running out of memory.
     * @param percent percent of available memory to use.
     * @param freePhysicalMemory amount of available physical memory on the host machine.
     * @param maxRuntimeMemory amount of memory this JVM can occupy.
     * @return the amount of off-heap memory available for allocation, based on the given {@code percent}.
     */
    public long calculateMaxAvailableOffHeapMemoryFromPercent(
            int percent, long freePhysicalMemory, long maxRuntimeMemory) {
        requireBetween(percent, 1, 100);

        if (freePhysicalMemory == OsBeanUtil.VALUE_UNAVAILABLE) {
            // Unable to detect amount of free memory, so rather max memory should be explicitly set
            // in order to get the best performance. However, let's just go with a default of 2G in this case.
            var defaultMemory = gibiBytes(2);
            monitor.unableToDetectMachineMemory(defaultMemory);
            return defaultMemory;
        }

        double factor = percent / 100D;
        // If the JVM max heap size (-Xmx) have been configured to use a significant portion of the machine memory
        // then it's not reasonable for running this tool, at the very least not desirable since the majority of
        // memory lives off-heap. So if this is the case then assume only half the memory is assigned to the JVM,
        // otherwise the tool performance could be massively crippled.
        long jvmMaxMemory = Math.min(maxRuntimeMemory, freePhysicalMemory / 2);
        long availableMemory = freePhysicalMemory - jvmMaxMemory;
        return round(availableMemory * factor);
    }

    /**
     * Calculates max amount of available off-heap memory, for e.g. a tool to use.
     * @param value can be an actual value in bytes, a value with a postfix e.g. 'k', 'm' or 'g',
     * or a percentage of how much of the available machine memory to use, e.g. "90%".
     * @return the amount of off-heap memory available for allocation, based on the given {@code percent}.
     */
    public long calculateMaxAvailableOffHeapMemory(String value) {
        value = value.trim();
        if (value.endsWith("%")) {
            int percent = Integer.parseInt(value.substring(0, value.length() - 1));
            return calculateMaxAvailableOffHeapMemoryFromPercent(percent);
        }
        return parseLongWithUnit(value);
    }

    public interface Monitor {
        void unableToDetectMachineMemory(long amountOfMemoryUsedInstead);
    }
}
