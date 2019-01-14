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

import static java.lang.Character.toUpperCase;
import static java.util.Objects.requireNonNull;

public abstract class HeapAllocation
{
    public static final HeapAllocation HEAP_ALLOCATION;
    public static final HeapAllocation NOT_AVAILABLE;

    static
    {
        NOT_AVAILABLE = new HeapAllocationNotAvailable(); // must be first!
        HEAP_ALLOCATION = load( ManagementFactory.getThreadMXBean() );
    }

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param thread
     *         the thread to get the used CPU time for.
     * @return number of allocated bytes for specified thread.
     */
    public final long allocatedBytes( Thread thread )
    {
        return allocatedBytes( thread.getId() );
    }

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param threadId
     *         the id of the thread to get the allocation information for.
     * @return number of allocated bytes for specified threadId.
     */
    public abstract long allocatedBytes( long threadId );

    private static HeapAllocation load( ThreadMXBean bean )
    {
        Class<HeapAllocation> base = HeapAllocation.class;
        StringBuilder name = new StringBuilder().append( base.getPackage().getName() ).append( '.' );
        String pkg = bean.getClass().getPackage().getName();
        int start = 0;
        int end = pkg.indexOf( '.', start );
        while ( end > 0 )
        {
            name.append( toUpperCase( pkg.charAt( start ) ) ).append( pkg.substring( start + 1, end ) );
            start = end + 1;
            end = pkg.indexOf( '.', start );
        }
        name.append( toUpperCase( pkg.charAt( start ) ) ).append( pkg.substring( start + 1 ) );
        name.append( base.getSimpleName() );
        try
        {
            return requireNonNull( (HeapAllocation) Class.forName( name.toString() )
                    .getDeclaredMethod( "load", ThreadMXBean.class )
                    .invoke( null, bean ), "Loader method returned null." );
        }
        catch ( Throwable e )
        {
            //noinspection ConstantConditions -- this can actually happen if the code order is wrong
            if ( NOT_AVAILABLE == null )
            {
                throw new LinkageError( "Bad code loading order.", e );
            }
            return NOT_AVAILABLE;
        }
    }

    private static class HeapAllocationNotAvailable extends HeapAllocation
    {
        @Override
        public long allocatedBytes( long threadId )
        {
            return -1;
        }
    }
}
