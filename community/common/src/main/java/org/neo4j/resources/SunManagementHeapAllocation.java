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

import com.sun.management.ThreadMXBean;

class SunManagementHeapAllocation extends HeapAllocation
{
    /**
     * Invoked from {@link HeapAllocation#load(java.lang.management.ThreadMXBean)} through reflection.
     */
    @SuppressWarnings( "unused" )
    static HeapAllocation load( java.lang.management.ThreadMXBean bean )
    {
        if ( ThreadMXBean.class.isInstance( bean ) )
        {
            return new SunManagementHeapAllocation( (ThreadMXBean) bean );
        }
        return NOT_AVAILABLE;
    }

    private final ThreadMXBean threadMXBean;

    private SunManagementHeapAllocation( ThreadMXBean threadMXBean )
    {
        this.threadMXBean = threadMXBean;
    }

    @Override
    public long allocatedBytes( long threadId )
    {
        if ( !threadMXBean.isThreadAllocatedMemorySupported() )
        {
            return -1;
        }
        if ( !threadMXBean.isThreadAllocatedMemoryEnabled() )
        {
            threadMXBean.setThreadAllocatedMemoryEnabled( true );
        }
        return threadMXBean.getThreadAllocatedBytes( threadId );
    }
}
