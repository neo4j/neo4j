/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

final class CursorPool extends ThreadLocal<CursorPool.CursorSets>
{
    private final MuninnPagedFile pagedFile;
    private final long victimPage;

    CursorPool( MuninnPagedFile pagedFile )
    {
        this.pagedFile = pagedFile;
        this.victimPage = pagedFile.pageCache.victimPage;
    }

    @Override
    protected CursorSets initialValue()
    {
        return new CursorSets();
    }

    MuninnReadPageCursor takeReadCursor( long pageId, int pf_flags )
    {
        CursorSets cursorSets = get();
        MuninnReadPageCursor cursor = cursorSets.readCursors;
        if ( cursor != null )
        {
            cursorSets.readCursors = cursor.nextCursor;
        }
        else
        {
            cursor = createReadCursor( cursorSets );
        }
        cursor.initialiseFlags( pagedFile, pageId, pf_flags );
        return cursor;
    }

    private MuninnReadPageCursor createReadCursor( CursorSets cursorSets )
    {
        MuninnReadPageCursor cursor = new MuninnReadPageCursor( cursorSets, victimPage );
        cursor.initialiseFile( pagedFile );
        return cursor;
    }

    MuninnWritePageCursor takeWriteCursor( long pageId, int pf_flags )
    {
        CursorSets cursorSets = get();
        MuninnWritePageCursor cursor = cursorSets.writeCursors;
        if ( cursor != null )
        {
            cursorSets.writeCursors = cursor.nextCursor;
        }
        else
        {
            cursor = createWriteCursor( cursorSets );
        }
        cursor.initialiseFlags( pagedFile, pageId, pf_flags );
        return cursor;
    }

    private MuninnWritePageCursor createWriteCursor( CursorSets cursorSets )
    {
        MuninnWritePageCursor cursor = new MuninnWritePageCursor( cursorSets, victimPage );
        cursor.initialiseFile( pagedFile );
        return cursor;
    }

    static class CursorSets
    {
        MuninnReadPageCursor readCursors;
        MuninnWritePageCursor writeCursors;
    }
}
