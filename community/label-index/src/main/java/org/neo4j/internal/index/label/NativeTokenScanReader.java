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
package org.neo4j.internal.index.label;

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;

import static org.neo4j.internal.index.label.TokenScanValue.RANGE_SIZE;
import static org.neo4j.internal.index.label.NativeTokenScanWriter.rangeOf;

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
    public PrimitiveLongResourceIterator nodesWithLabel( int tokenId, PageCursorTracer cursorTracer )
    {
        Seeker<TokenScanKey,TokenScanValue> cursor;
        try
        {
            cursor = seekerForLabel( 0, tokenId, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        return new LabelScanValueIterator( cursor, TokenScanReader.NO_ID );
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithAnyOfLabels( long fromId, int[] tokenIds, PageCursorTracer cursorTracer )
    {
        List<PrimitiveLongResourceIterator> iterators = iteratorsForLabels( fromId, cursorTracer, tokenIds );
        return new CompositeTokenScanValueIterator( iterators, false );
    }

    @Override
    public LabelScan nodeLabelScan( int tokenId, PageCursorTracer cursorTracer )
    {
        try
        {
            long highestEntityIdForToken = highestNodeIdForLabel( tokenId, cursorTracer );
            return new NativeLabelScan( tokenId, highestEntityIdForToken );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private long highestNodeIdForLabel( int tokenId, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Seeker<TokenScanKey,TokenScanValue> seeker = index.seek( new TokenScanKey( tokenId, Long.MAX_VALUE ),
                new TokenScanKey( tokenId, Long.MIN_VALUE ), cursorTracer ) )
        {
            return seeker.next() ? (seeker.key().idRange + 1) * RANGE_SIZE : 0;
        }
    }

    private List<PrimitiveLongResourceIterator> iteratorsForLabels( long fromId, PageCursorTracer cursorTracer, int[] tokenIds )
    {
        List<PrimitiveLongResourceIterator> iterators = new ArrayList<>();
        try
        {
            for ( int tokenId : tokenIds )
            {
                Seeker<TokenScanKey,TokenScanValue> cursor = seekerForLabel( fromId, tokenId, cursorTracer );
                iterators.add( new LabelScanValueIterator( cursor, fromId ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return iterators;
    }

    private Seeker<TokenScanKey,TokenScanValue> seekerForLabel( long startId, int tokenId, PageCursorTracer cursorTracer ) throws IOException
    {
        TokenScanKey from = new TokenScanKey( tokenId, rangeOf( startId ) );
        TokenScanKey to = new TokenScanKey( tokenId, Long.MAX_VALUE );
        return index.seek( from, to, cursorTracer );
    }

    private Seeker<TokenScanKey,TokenScanValue> seekerForLabel( long startId, long stopId, int tokenId, PageCursorTracer cursorTracer ) throws IOException
    {
        TokenScanKey from = new TokenScanKey( tokenId, rangeOf( startId ) );
        TokenScanKey to = new TokenScanKey( tokenId, rangeOf( stopId ) );

        return index.seek( from, to, cursorTracer );
    }

    private class NativeLabelScan implements LabelScan
    {
        private final AtomicLong nextStart;
        private final int tokenId;
        private final long max;

        NativeLabelScan( int tokenId, long max )
        {
            this.tokenId = tokenId;
            this.max = max;
            nextStart = new AtomicLong( 0 );
        }

        @Override
        public IndexProgressor initialize( IndexProgressor.NodeLabelClient client, PageCursorTracer cursorTracer )
        {
            return init( client, 0L, Long.MAX_VALUE, cursorTracer );
        }

        @Override
        public IndexProgressor initializeBatch( IndexProgressor.NodeLabelClient client, int sizeHint, PageCursorTracer cursorTracer )
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
            return init( client, start, stop, cursorTracer );
        }

        private IndexProgressor init( IndexProgressor.NodeLabelClient client, long start, long stop, PageCursorTracer cursorTracer )
        {
            Seeker<TokenScanKey,TokenScanValue> cursor;
            try
            {
                cursor = seekerForLabel( start, stop, tokenId, cursorTracer );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            return new LabelScanValueIndexProgressor( cursor, client );
        }

        private long roundUp( long sizeHint )
        {
            return (sizeHint / RANGE_SIZE + 1) * RANGE_SIZE;
        }
    }
}
