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
package org.neo4j.memory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import org.neo4j.io.os.OsBeanUtil;

public interface MachineMemory {
    /**
     * @return total amount of physical memory in bytes, or -1 if the functionality is not supported.
     */
    long getTotalPhysicalMemory();

    /**
     * @return a {@link MemoryUsage} object representing
     * the heap memory usage.
     */
    MemoryUsage getHeapMemoryUsage();

    /**
     * @return true if the running JVM uses compressed oops
     */
    boolean hasCompressedOOPS();

    MachineMemory DEFAULT = new MachineMemory() {
        @Override
        public long getTotalPhysicalMemory() {
            return OsBeanUtil.getTotalPhysicalMemory();
        }

        @Override
        public MemoryUsage getHeapMemoryUsage() {
            return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        }

        @Override
        public boolean hasCompressedOOPS() {
            return HeapEstimator.hasCompressedOOPS();
        }
    };
}
