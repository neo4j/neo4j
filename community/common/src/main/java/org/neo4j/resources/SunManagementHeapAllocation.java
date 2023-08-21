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

import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;

import com.sun.management.ThreadMXBean;

final class SunManagementHeapAllocation extends HeapAllocation {
    /**
     * Invoked from {@code HeapAllocation#load(java.lang.management.ThreadMXBean)} through reflection.
     */
    @SuppressWarnings("unused")
    static HeapAllocation load(java.lang.management.ThreadMXBean bean) {
        checkArgument(
                bean instanceof ThreadMXBean,
                "The ThreadMXBean must be an instance of '" + ThreadMXBean.class.getName() + "'.");
        ThreadMXBean threadMXBean = (ThreadMXBean) bean;
        checkState(threadMXBean.isThreadAllocatedMemorySupported(), "Thread allocations not supported.");
        return new SunManagementHeapAllocation(threadMXBean);
    }

    private final ThreadMXBean threadMXBean;

    private SunManagementHeapAllocation(ThreadMXBean threadMXBean) {
        this.threadMXBean = threadMXBean;
        if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
            threadMXBean.setThreadAllocatedMemoryEnabled(true);
        }
    }

    @Override
    public long allocatedBytes(long threadId) {
        return threadMXBean.getThreadAllocatedBytes(threadId);
    }
}
