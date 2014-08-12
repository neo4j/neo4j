/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.enterprise.pagecache.impl.muninn;

abstract class MuninnCursorFreelist
{
    private static final int MAX_CURSORS_PER_FREELIST = 1000;

    static class CursorRef
    {
        public MuninnPageCursor cursor;
        // We keep a count of how many cursors are in the freelist, so that the
        // number of cursors we keep is bounded. We do this to deal with the odd
        // case where one thread takes cursors off its freelist, and then gives
        // them to another thread for processing or returning. If we didn't keep
        // a bound on the freelist size, that other thread would collect cursors
        // until the process ran out of memory.
        // Ideally, a cursor should always be taken and returned by the same
        // thread.
        public int counter = 0;
    }

    private static class CursorRefThreadLocal extends ThreadLocal<CursorRef>
    {
        @Override
        protected CursorRef initialValue()
        {
            return new CursorRef();
        }
    }

    private final ThreadLocal<CursorRef> freelist = new CursorRefThreadLocal();

    public MuninnPageCursor takeCursor()
    {
        CursorRef ref = freelist.get();
        MuninnPageCursor cursor = ref.cursor;
        if ( cursor == null )
        {
            return createNewCursor();
        }
        ref.cursor = cursor.nextFree;
        ref.counter--;
        return cursor;
    }

    protected abstract MuninnPageCursor createNewCursor();

    public void returnCursor( MuninnPageCursor cursor )
    {
        CursorRef ref = freelist.get();
        if ( ref.counter < MAX_CURSORS_PER_FREELIST )
        {
            cursor.nextFree = ref.cursor;
            ref.cursor = cursor;
            ref.counter++;
        }
    }
}
