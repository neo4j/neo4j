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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.PageCache;

import static java.lang.String.format;
import static org.neo4j.helpers.Format.bytes;

public class PageCacheArrayFactoryMonitor implements NumberArrayFactory.Monitor
{
    // This field is designed to allow multiple threads setting it concurrently, where one of those will win and either one is fine
    // because this monitor mostly revolves around highlighting the fact that the page cache number array is in use at all.
    private final AtomicReference<String> failedFactoriesDescription = new AtomicReference<>();

    @Override
    public void allocationSuccessful( long memory, NumberArrayFactory successfulFactory,
            Iterable<NumberArrayFactory.AllocationFailure> attemptedAllocationFailures )
    {
        if ( successfulFactory instanceof PageCachedNumberArrayFactory )
        {
            StringBuilder builder =
                    new StringBuilder( format( "Memory allocation of %s ended up in page cache, which may impact performance negatively", bytes( memory ) ) );
            attemptedAllocationFailures.forEach(
                    failure -> builder.append( format( "%n%s: %s", failure.getFactory(), failure.getFailure() ) ) );
            failedFactoriesDescription.compareAndSet( null, builder.toString() );
        }
    }

    /**
     * Called by user-facing progress monitor at arbitrary points to get information about whether or not there has been
     * one or more {@link NumberArrayFactory} allocations backed by the {@link PageCache}, this because it severely affects
     * performance. Calling this method clears the failure description, if any.
     *
     * @return if there have been {@link NumberArrayFactory} allocations backed by the {@link PageCache} since the last call to this method
     * then a description of why it was chosen is returned, otherwise {@code null}.
     */
    public String pageCacheAllocationOrNull()
    {
        String failure = failedFactoriesDescription.get();
        if ( failure != null )
        {
            failedFactoriesDescription.compareAndSet( failure, null );
        }
        return failure;
    }
}
