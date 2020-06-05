/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.pagecache.tracing;

import org.neo4j.io.pagecache.PageSwapper;

/**
 * Represents the opportunity to flush a page.
 *
 * The flushing might not happen, though, because only dirty pages are flushed.
 */
public interface FlushEventOpportunity
{
    /**
     * A FlushEventOpportunity that only returns the FlushEvent.NULL.
     */
    FlushEventOpportunity NULL = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper, int pagesToFlush, int mergedPages )
        {
            return FlushEvent.NULL;
        }

        @Override
        public void startFlush( int[][] translationTable )
        {

        }

        @Override
        public ChunkEvent startChunk( int[] chunk )
        {
            return ChunkEvent.NULL;
        }
    };

    /**
     * Begin flushing the given page.
     */
    FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper, int pagesToFlush, int mergedPages );

    /**
     * Start flushing of given translation table
     * @param translationTable table we flush
     */
    void startFlush( int[][] translationTable );

    /**
     * Start flushing of given chunk
     * @param chunk chunk we start flushing
     */
    ChunkEvent startChunk( int[] chunk );

    /**
     * Event generated during translation table chunk flushing from memory to backing file
     */
    class ChunkEvent
    {
        public static final ChunkEvent NULL = new ChunkEvent();

        protected ChunkEvent()
        {
        }

        public void chunkFlushed( long notModifiedPages, long flushPerChunk, long buffersPerChunk, long mergesPerChunk )
        {
        }
    }
}
