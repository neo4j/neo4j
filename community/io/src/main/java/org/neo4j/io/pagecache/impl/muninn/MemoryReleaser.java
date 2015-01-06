/**
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
package org.neo4j.io.pagecache.impl.muninn;

/**
 * This class has a finalizer. It's purpose is to free all the native memory
 * pointers when we are done using them. This is implemented by having every
 * page have a reference to this memory releaser, and register their pointers
 * with it. When the pool shuts down, it drops all of its references to the
 * pages, and when the PageCursor is done with a page, it too drops the
 * reference. This way, when the cache is closed and all the cursors are
 * closed, nothing will be left to keep the pages alive, and so this memory
 * releaser will transitively also become garbage. Then the finalizer method
 * will run, and free all the native memory.
 */
final class MemoryReleaser
{
    private final long[] rawPointers;

    public MemoryReleaser( int maxPages )
    {
        this.rawPointers = new long[maxPages];
    }

    @Override
    protected void finalize() throws Throwable
    {
        int length = rawPointers.length;
        for ( int i = 0; i < length; i++ )
        {
            long pointer = rawPointers[i];
            rawPointers[i] = 0;
            UnsafeUtil.free( pointer );
        }
        super.finalize();
    }

    public void registerPointer( int cachePageId, long pointer )
    {
        // Note: this method is accessed concurrently by many threads as they
        // fault on the cache pages for their first time. Luckily, the JMM
        // forbids word-tearing, so access to adjacent elements of an array
        // need no special synchronisation.
        rawPointers[cachePageId] = pointer;
    }
}
