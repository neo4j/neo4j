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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;

import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.index.label.AllEntriesTokenScanReader;
import org.neo4j.internal.index.label.EntityTokenRange;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Inserts empty {@link EntityTokenRange} for those ranges missing from the source iterator.
 * High entity id is known up front such that ranges are returned up to that point.
 */
class GapFreeAllEntriesTokenScanReader implements AllEntriesTokenScanReader
{
    private static final String GAP_FREE_ALL_ENTRIES_READER_TAG = "gapFreeAllEntriesReader";
    private final AllEntriesTokenScanReader entityTokenRanges;
    private final long highId;
    private final EntityType entityType;
    private final PageCursorTracer cursorTracer;

    GapFreeAllEntriesTokenScanReader( TokenScanStore scanStore, long highId, PageCacheTracer cacheTracer )
    {
        this.cursorTracer = cacheTracer.createPageCursorTracer( GAP_FREE_ALL_ENTRIES_READER_TAG );
        this.entityTokenRanges = scanStore.allEntityTokenRanges( cursorTracer );
        this.highId = highId;
        this.entityType = scanStore.entityType();
    }

    @Override
    public long maxCount()
    {
        return entityTokenRanges.maxCount();
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( entityTokenRanges, cursorTracer );
    }

    @Override
    public int rangeSize()
    {
        return entityTokenRanges.rangeSize();
    }

    @Override
    public Iterator<EntityTokenRange> iterator()
    {
        long highestRangeId = 0;
        int rangeSize = entityTokenRanges.rangeSize();
        if ( rangeSize != 0 )
        {
            highestRangeId = (highId - 1 ) / rangeSize;
        }
        return new GapFillingIterator( entityTokenRanges.iterator(), highestRangeId, rangeSize, entityType );
    }

    private static class GapFillingIterator extends PrefetchingIterator<EntityTokenRange>
    {
        private final long highestRangeId;
        private final Iterator<EntityTokenRange> source;
        private final long[][] emptyRangeData;
        private final EntityType entityType;

        private EntityTokenRange nextFromSource;
        private long currentRangeId = -1;

        GapFillingIterator( Iterator<EntityTokenRange> entityTokenRangeIterator, long highestRangeId, int rangeSize, EntityType entityType )
        {
            this.highestRangeId = highestRangeId;
            this.source = entityTokenRangeIterator;
            this.emptyRangeData = new long[rangeSize][];
            this.entityType = entityType;
        }

        @Override
        protected EntityTokenRange fetchNextOrNull()
        {
            while ( true )
            {
                // These conditions only come into play after we've gotten the first range from the source
                if ( nextFromSource != null )
                {
                    if ( currentRangeId + 1 == nextFromSource.id() )
                    {
                        // Next to return is the one from source
                        currentRangeId++;
                        return nextFromSource;
                    }

                    if ( currentRangeId < nextFromSource.id() )
                    {
                        // Source range iterator has a gap we need to fill
                        return new EntityTokenRange( ++currentRangeId, emptyRangeData, entityType );
                    }
                }

                if ( source.hasNext() )
                {
                    // The source iterator has more ranges, grab the next one
                    nextFromSource = source.next();
                    // continue in the outer loop
                }
                else if ( currentRangeId < highestRangeId )
                {
                    nextFromSource = new EntityTokenRange( highestRangeId, emptyRangeData, entityType );
                    // continue in the outer loop
                }
                else
                {
                    // End has been reached
                    return null;
                }
            }
        }
    }
}
