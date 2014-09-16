/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

class MemoryReleaser
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
        rawPointers[cachePageId] = pointer;
    }
}
