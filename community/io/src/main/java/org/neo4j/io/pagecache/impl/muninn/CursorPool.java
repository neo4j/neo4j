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
package org.neo4j.io.pagecache.impl.muninn;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

final class CursorPool
{
    private static boolean disableCursorPooling = flag( CursorPool.class, "disableCursorPooling", false );

    private final ThreadLocal<MuninnReadPageCursor> readCursorCache = new MuninnReadPageCursorThreadLocal();
    private final ThreadLocal<MuninnWritePageCursor> writeCursorCache = new MuninnWritePageCursorThreadLocal();

    public MuninnReadPageCursor takeReadCursor()
    {
        if ( disableCursorPooling )
        {
            return new MuninnReadPageCursor();
        }

        MuninnReadPageCursor cursor = readCursorCache.get();

        assert unclaimed( cursor, writeCursorCache );

        cursor.markAsClaimed();
        return cursor;
    }

    public MuninnWritePageCursor takeWriteCursor()
    {
        if ( disableCursorPooling )
        {
            return new MuninnWritePageCursor();
        }

        MuninnWritePageCursor cursor = writeCursorCache.get();

        assert unclaimed( cursor, readCursorCache );

        cursor.markAsClaimed();
        return cursor;
    }

    private static boolean unclaimed( MuninnPageCursor first, ThreadLocal<? extends MuninnPageCursor> second )
    {
        // This has been pulled out into a static method and guarded behind
        // `assert`s to reduce the overhead put upon the inlining budget
        // by something that should never happen.
        first.assertUnclaimed();
        second.get().assertUnclaimed();
        return true;
    }

    private static class MuninnReadPageCursorThreadLocal extends ThreadLocal<MuninnReadPageCursor>
    {
        @Override
        protected MuninnReadPageCursor initialValue()
        {
            return new MuninnReadPageCursor();
        }
    }

    private static class MuninnWritePageCursorThreadLocal extends ThreadLocal<MuninnWritePageCursor>
    {
        @Override
        protected MuninnWritePageCursor initialValue()
        {
            return new MuninnWritePageCursor();
        }
    }
}
