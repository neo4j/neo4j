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
package org.neo4j.resources;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public abstract class HeapAllocation {
    public static final HeapAllocation NOT_AVAILABLE = new HeapAllocationNotAvailable();
    public static final HeapAllocation HEAP_ALLOCATION = tryLoad(ManagementFactory.getThreadMXBean(), NOT_AVAILABLE);

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param thread
     *         the thread to get the used CPU time for.
     * @return number of allocated bytes for specified thread.
     */
    public final long allocatedBytes(Thread thread) {
        return allocatedBytes(thread.getId());
    }

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param threadId
     *         the id of the thread to get the allocation information for.
     * @return number of allocated bytes for specified threadId.
     */
    public abstract long allocatedBytes(long threadId);

    /**
     * We use reflection to reference the classes. Mentioning {@code HotSpotThreadImpl} or {@code SunManagementHeapAllocation} as class
     * references would cause a class loader error on VMs that do not support that.
     */
    @SuppressWarnings("SameParameterValue")
    private static HeapAllocation tryLoad(ThreadMXBean bean, HeapAllocation fallback) {
        try {
            if (bean.getClass().getName().equals("com.sun.management.internal.HotSpotThreadImpl")) {
                return (HeapAllocation) Class.forName("org.neo4j.resources.SunManagementHeapAllocation")
                        .getDeclaredMethod("load", ThreadMXBean.class)
                        .invoke(null, bean);
            }
        } catch (Throwable e) {
            // We are on a VM that don't support this, use the fallback
        }
        return fallback;
    }

    private static class HeapAllocationNotAvailable extends HeapAllocation {
        @Override
        public long allocatedBytes(long threadId) {
            return -1;
        }
    }
}
