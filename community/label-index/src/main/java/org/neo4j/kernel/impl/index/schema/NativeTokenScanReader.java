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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.kernel.impl.index.schema.NativeTokenScanWriter.rangeOf;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

/**
 * {@link TokenScanReader} for reading data from {@link NativeTokenScanStore}.
 * Each {@link LongIterator} returned from each of the methods has a {@link Seeker}
 * directly from {@link GBPTree#seek(Object, Object, PageCursorTracer)} backing it.
 */
class NativeTokenScanReader implements TokenScanReader
{
    /**
     * Index that is queried when calling the methods below.
     */
    private final GBPTree<TokenScanKey,TokenScanValue> index;

    NativeTokenScanReader( GBPTree<TokenScanKey,TokenScanValue> index )
    {
        this.index = index;
    }

    @Override
    public PrimitiveLongResourceIterator entitiesWithToken( int tokenId, PageCursorTracer cursorTracer )
    {
        Seeker<TokenScanKey,TokenScanValue> cursor;
        try
        {
            cursor = seekerForToken( 0, tokenId, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        return new TokenScanValueIterator( cursor, TokenScanReader.NO_ID );
    }

    @Override
    public PrimitiveLongResourceIterator entitiesWithAnyOfTokens( long fromId, int[] tokenIds, PageCursorTracer cursorTracer )
    {
        List<PrimitiveLongResourceIterator> iterators = iteratorsForTokens( fromId, cursorTracer, tokenIds );
        return new CompositeTokenScanValueIterator( iterators, false );
    }

    @Override
    public TokenScan entityTokenScan( int tokenId, PageCursorTracer cursorTracer )
    {
        try
        {
            long highestEntityIdForToken = highestEntityIdForToken( tokenId, cursorTracer );
            return new NativeTokenScan( tokenId, highestEntityIdForToken );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private long highestEntityIdForToken( int tokenId, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Seeker<TokenScanKey,TokenScanValue> seeker = index.seek( new TokenScanKey( tokenId, Long.MAX_VALUE ),
                new TokenScanKey( tokenId, Long.MIN_VALUE ), cursorTracer ) )
        {
            return seeker.next() ? (seeker.key().idRange + 1) * RANGE_SIZE : 0;
        }
    }

    private List<PrimitiveLongResourceIterator> iteratorsForTokens( long fromId, PageCursorTracer cursorTracer, int[] tokenIds )
    {
        List<PrimitiveLongResourceIterator> iterators = new ArrayList<>();
        try
        {
            for ( int tokenId : tokenIds )
            {
                Seeker<TokenScanKey,TokenScanValue> cursor = seekerForToken( fromId, tokenId, cursorTracer );
                iterators.add( new TokenScanValueIterator( cursor, fromId ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return iterators;
    }

    private Seeker<TokenScanKey,TokenScanValue> seekerForToken( long startId, int tokenId, PageCursorTracer cursorTracer ) throws IOException
    {
        return seekerForToken( startId, Long.MAX_VALUE, tokenId, IndexOrder.NONE, cursorTracer );
    }

    private Seeker<TokenScanKey,TokenScanValue> seekerForToken( long startId, long stopId, int tokenId, IndexOrder indexOrder, PageCursorTracer cursorTracer )
            throws IOException
    {
        long rangeFrom;
        long rangeTo;
        if ( indexOrder != IndexOrder.DESCENDING )
        {
            rangeFrom = startId;
            rangeTo = stopId;
        }
        else
        {
            rangeFrom = stopId;
            rangeTo = startId;
        }

        TokenScanKey fromKey = new TokenScanKey( tokenId, rangeOf( rangeFrom ) );
        TokenScanKey toKey = new TokenScanKey( tokenId, rangeOf( rangeTo ) );
        return index.seek( fromKey, toKey, cursorTracer );
    }

    @VisibleForTesting
    static long roundUp( long sizeHint )
    {
        return ((sizeHint + RANGE_SIZE - 1) / RANGE_SIZE) * RANGE_SIZE;
    }

    private class NativeTokenScan implements TokenScan
    {
        private final AtomicLong nextStart;
        private final int tokenId;
        private final long max;

        NativeTokenScan( int tokenId, long max )
        {
            this.tokenId = tokenId;
            this.max = max;
            nextStart = new AtomicLong( 0 );
        }

        @Override
        public IndexProgressor initialize( IndexProgressor.EntityTokenClient client, IndexOrder indexOrder, PageCursorTracer cursorTracer )
        {
            return init( client, Long.MIN_VALUE, Long.MAX_VALUE, indexOrder, cursorTracer );
        }

        @Override
        public IndexProgressor initializeBatch( IndexProgressor.EntityTokenClient client, int sizeHint, PageCursorTracer cursorTracer )
        {
            if ( sizeHint == 0 )
            {
                return IndexProgressor.EMPTY;
            }
            long size = roundUp( sizeHint );
            long start = nextStart.getAndAdd( size );
            long stop = Math.min( start + size, max );
            if ( start >= max )
            {
                return IndexProgressor.EMPTY;
            }
            return init( client, start, stop, IndexOrder.NONE, cursorTracer );
        }

        private IndexProgressor init( IndexProgressor.EntityTokenClient client, long start, long stop, IndexOrder indexOrder, PageCursorTracer cursorTracer )
        {
            Seeker<TokenScanKey,TokenScanValue> cursor;
            try
            {
                cursor = seekerForToken( start, stop, tokenId, indexOrder, cursorTracer );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            return new TokenScanValueIndexProgressor( cursor, client, indexOrder );
        }
    }
}
