/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.resources;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Measures CPU time by thread.
 */
public abstract class CpuClock
{
    public static final CpuClock CPU_CLOCK = new CpuClock()
    {
        private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        @Override
        public long cpuTimeNanos( long threadId )
        {
            if ( !threadMXBean.isThreadCpuTimeSupported() )
            {
                return -1;
            }
            if ( !threadMXBean.isThreadCpuTimeEnabled() )
            {
                threadMXBean.setThreadCpuTimeEnabled( true );
            }
            return threadMXBean.getThreadCpuTime( threadId );
        }
    };
    public static final CpuClock NOT_AVAILABLE = new CpuClock()
    {
        @Override
        public long cpuTimeNanos( long threadId )
        {
            return -1;
        }
    };

    /**
     * Returns the current CPU time used by the thread, in nanoseconds.
     *
     * @param thread
     *         the thread to get the used CPU time for.
     * @return the current CPU time used by the thread, in nanoseconds.
     */
    public final long cpuTimeNanos( Thread thread )
    {
        return cpuTimeNanos( thread.getId() );
    }

    /**
     * Returns the current CPU time used by the thread, in nanoseconds.
     *
     * @param threadId
     *         the id of the thread to get the used CPU time for.
     * @return the current CPU time used by the thread, in nanoseconds, or {@code -1} if getting the CPU time is not
     * supported.
     */
    public abstract long cpuTimeNanos( long threadId );
}
