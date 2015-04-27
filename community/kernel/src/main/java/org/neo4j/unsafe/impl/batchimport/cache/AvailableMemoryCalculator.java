/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.lang.management.ManagementFactory;

/**
 * Calculates how much heap and off-heap memory there are left to allocate in the JVM.
 */
public interface AvailableMemoryCalculator
{
    long availableHeapMemory();

    long availableOffHeapMemory();

    /**
     * Uses {@link Runtime#maxMemory()} and {@link ManagementFactory#getOperatingSystemMXBean()} to provide
     * memory information to do free memory calculations on.
     */
    public static final AvailableMemoryCalculator RUNTIME = new AvailableMemoryCalculator()
    {
        @Override
        public long availableOffHeapMemory()
        {
            return osBean().getFreePhysicalMemorySize();
        }

        private com.sun.management.OperatingSystemMXBean osBean()
        {
            com.sun.management.OperatingSystemMXBean bean =
                    (com.sun.management.OperatingSystemMXBean)
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            return bean;
        }

        @Override
        public long availableHeapMemory()
        {
            Runtime runtime = runtime();
            return runtime.maxMemory() - runtime.totalMemory();
        }

        private Runtime runtime()
        {
            System.gc();
            Runtime runtime = Runtime.getRuntime();
            return runtime;
        }

        @Override
        public String toString()
        {
            return "Java runtime memory calculator";
        }
    };
}
