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
package org.neo4j.index.gbptree;

import org.neo4j.io.pagecache.PageCursor;

class TreeState
{
    private final long pageId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final long rootId;
    private final long lastId;
    private final boolean valid;

    TreeState( long pageId, long stableGeneration, long unstableGeneration, long rootId, long lastId, boolean valid )
    {
        this.pageId = pageId;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.rootId = rootId;
        this.lastId = lastId;
        this.valid = valid;
    }

    long pageId()
    {
        return pageId;
    }

    long stableGeneration()
    {
        return stableGeneration;
    }

    long unstableGeneration()
    {
        return unstableGeneration;
    }

    long rootId()
    {
        return rootId;
    }

    long lastId()
    {
        return lastId;
    }

    boolean isValid()
    {
        return valid;
    }

    static void write( PageCursor cursor, long stableGeneration, long unstableGeneration, long rootId,
            long lastId )
    {
        GenSafePointer.assertGenerationOnWrite( stableGeneration );
        GenSafePointer.assertGenerationOnWrite( unstableGeneration );

        writeStateOnce( cursor, stableGeneration, unstableGeneration, rootId, lastId ); // Write state
        writeStateOnce( cursor, stableGeneration, unstableGeneration, rootId, lastId ); // Write checksum
    }

    static TreeState read( PageCursor cursor )
    {
        long pageId = cursor.getCurrentPageId();

        long stableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long unstableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long rootId = cursor.getLong();
        long lastId = cursor.getLong();

        long checksumStableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long checksumUnstableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long checksumRootId = cursor.getLong();
        long checksumLastId = cursor.getLong();

        boolean valid = stableGeneration == checksumStableGeneration &&
                        unstableGeneration == checksumUnstableGeneration &&
                        rootId == checksumRootId &&
                        lastId == checksumLastId;

        boolean isEmpty = stableGeneration == 0L && unstableGeneration == 0L && rootId == 0L && lastId == 0L;
        valid &= !isEmpty;

        return new TreeState( pageId, stableGeneration, unstableGeneration, rootId, lastId, valid );
    }

    private static void writeStateOnce( PageCursor cursor, long stableGeneration, long unstableGeneration,
            long rootId, long lastId )
    {
        cursor.putInt( (int) stableGeneration );
        cursor.putInt( (int) unstableGeneration );
        cursor.putLong( rootId );
        cursor.putLong( lastId );
    }

    @Override
    public String toString()
    {
        return String.format( "pageId=%d, stableGeneration=%d, unstableGeneration=%d, rootId=%s, lastId=%d, valid=%b",
                pageId, stableGeneration, unstableGeneration, rootId, lastId, valid );
    }
}
