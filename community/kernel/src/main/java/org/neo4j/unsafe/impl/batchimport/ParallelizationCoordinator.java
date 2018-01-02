/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.graphdb.Resource;

/**
 * Lock for coordinating concurrency of execution between stretches of parallelizable batches
 * versus non-parallelizable batches. Usage is to
 * {@link #coordinate(boolean) lock based on whether or not being parallelizable with the previous batch},
 * put that {@link Resource} in a try-with-resource block, process and then exit that try-block.
 */
public class ParallelizationCoordinator
{
    private final ReadWriteLock lock = new ReentrantReadWriteLock( true );

    public Resource coordinate( boolean parallelizableWithPrevious )
    {
        if ( !parallelizableWithPrevious )
        {
            // If this batch isn't parallelizable with previous batch then we need to wait for all previous
            // batches (potentially many concurrent) to complete before this batch can run.
            // Here that translates into acquiring the write lock.
            lock.writeLock().lock();
        }

        // Now acquire the read lock, even if we have the write lock. Why? read right below.
        final Lock readLock = lock.readLock();
        readLock.lock();

        if ( !parallelizableWithPrevious )
        {
            // Alright, we've made our point above when we acquired the write lock, i.e. awaited previous
            // batches to complete and blocking new batches from starting. We can now
            // (after having acquired the read lock) release the write lock since:
            // - if the next batch isn't parallelizable with this batch it will await this batch to complete
            //   as a side effect of acquiring the write lock.
            // - if the next batch is parallelizable with this batch it can go right ahead and process
            //   since it'll only need to acquire the read lock.
            lock.writeLock().unlock();
        }

        return new Resource()
        {
            @Override
            public void close()
            {
                readLock.unlock();
            }
        };
    }
}
