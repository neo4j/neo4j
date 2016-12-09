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

import java.util.Objects;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

class TreeState
{
    private final long pageId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final long rootId;

    /**
     * Generation of {@link #rootId}.
     */
    private final long rootGen;

    /**
     * Highest allocated page id in the store. This id may not be in use currently and cannot decrease
     * since {@link PageCache} doesn't allow shrinking files.
     */
    private final long lastId;
    private final long freeListWritePageId;
    private final long freeListReadPageId;
    private final int freeListWritePos;
    private final int freeListReadPos;
    private boolean valid;

    TreeState( long pageId, long stableGeneration, long unstableGeneration, long rootId, long rootGen, long lastId,
            long freeListWritePageId, long freeListReadPageId, int freeListWritePos, int freeListReadPos,
            boolean valid )
    {
        this.pageId = pageId;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.rootId = rootId;
        this.rootGen = rootGen;
        this.lastId = lastId;
        this.freeListWritePageId = freeListWritePageId;
        this.freeListReadPageId = freeListReadPageId;
        this.freeListWritePos = freeListWritePos;
        this.freeListReadPos = freeListReadPos;
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

    long rootGen()
    {
        return rootGen;
    }

    long lastId()
    {
        return lastId;
    }

    long freeListWritePageId()
    {
        return freeListWritePageId;
    }

    long freeListReadPageId()
    {
        return freeListReadPageId;
    }

    int freeListWritePos()
    {
        return freeListWritePos;
    }

    int freeListReadPos()
    {
        return freeListReadPos;
    }

    boolean isValid()
    {
        return valid;
    }

    /**
     * Writes provided tree state to {@code cursor} at its current offset. Two versions of the state
     * are written after each other, the second one acting as checksum for the first, see {@link #valid} field.
     *
     * @param cursor {@link PageCursor} to write into, at its current offset.
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @param rootId root id.
     * @param rootGen root generation.
     * @param lastId last id.
     * @param freeListWritePageId free-list page id to write released ids into.
     * @param freeListReadPageId free-list page id to read released ids from.
     * @param freeListWritePos offset into free-list write page id to write released ids into.
     * @param freeListReadPos offset into free-list read page id to read released ids from.
     */
    static void write( PageCursor cursor, long stableGeneration, long unstableGeneration, long rootId, long rootGen,
            long lastId, long freeListWritePageId, long freeListReadPageId, int freeListWritePos,
            int freeListReadPos )
    {
        GenSafePointer.assertGenerationOnWrite( stableGeneration );
        GenSafePointer.assertGenerationOnWrite( unstableGeneration );

        writeStateOnce( cursor, stableGeneration, unstableGeneration, rootId, rootGen, lastId,
                freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos ); // Write state
        writeStateOnce( cursor, stableGeneration, unstableGeneration, rootId, rootGen, lastId,
                freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos ); // Write checksum
    }

    static TreeState read( PageCursor cursor )
    {
        TreeState state = readStateOnce( cursor );
        TreeState checksumState = readStateOnce( cursor );

        boolean valid = state.equals( checksumState );

        boolean isEmpty = state.isEmpty();
        valid &= !isEmpty;

        return state.setValid( valid );
    }

    private TreeState setValid( boolean valid )
    {
        this.valid = valid;
        return this;
    }

    private boolean isEmpty()
    {
        return stableGeneration == 0L && unstableGeneration == 0L && rootId == 0L && lastId == 0L &&
                freeListWritePageId == 0L && freeListReadPageId == 0L && freeListWritePos == 0 && freeListReadPos == 0;
    }

    private static TreeState readStateOnce( PageCursor cursor )
    {
        long pageId = cursor.getCurrentPageId();
        long stableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long unstableGeneration = cursor.getInt() & GenSafePointer.GENERATION_MASK;
        long rootId = cursor.getLong();
        long rootGen = cursor.getLong();
        long lastId = cursor.getLong();
        long freeListWritePageId = cursor.getLong();
        long freeListReadPageId = cursor.getLong();
        int freeListWritePos = cursor.getInt();
        int freeListReadPos = cursor.getInt();
        return new TreeState( pageId, stableGeneration, unstableGeneration, rootId, rootGen, lastId,
                freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos, true );
    }

    private static void writeStateOnce( PageCursor cursor, long stableGeneration, long unstableGeneration, long rootId,
            long rootGen, long lastId, long freeListWritePageId, long freeListReadPageId, int freeListWritePos,
            int freeListReadPos )
    {
        cursor.putInt( (int) stableGeneration );
        cursor.putInt( (int) unstableGeneration );
        cursor.putLong( rootId );
        cursor.putLong( rootGen );
        cursor.putLong( lastId );
        cursor.putLong( freeListWritePageId );
        cursor.putLong( freeListReadPageId );
        cursor.putInt( freeListWritePos );
        cursor.putInt( freeListReadPos );
    }

    @Override
    public String toString()
    {
        return String.format( "pageId=%d, stableGeneration=%d, unstableGeneration=%d, rootId=%d, rootGen=%d" +
                "lastId=%d, freeListWritePageId=%d, freeListReadPageId=%d, freeListWritePos=%d, freeListReadPos=%d, " +
                "valid=%b",
                pageId, stableGeneration, unstableGeneration, rootId, rootGen, lastId,
                freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos, valid );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        TreeState treeState = (TreeState) o;
        return pageId == treeState.pageId &&
                stableGeneration == treeState.stableGeneration &&
                unstableGeneration == treeState.unstableGeneration &&
                rootId == treeState.rootId &&
                rootGen == treeState.rootGen &&
                lastId == treeState.lastId &&
                freeListWritePageId == treeState.freeListWritePageId &&
                freeListReadPageId == treeState.freeListReadPageId &&
                freeListWritePos == treeState.freeListWritePos &&
                freeListReadPos == treeState.freeListReadPos &&
                valid == treeState.valid;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pageId, stableGeneration, unstableGeneration, rootId, rootGen, lastId,
                freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos, valid );
    }
}
