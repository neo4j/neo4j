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
package org.neo4j.kernel.api.impl.labelscan.storestrategy;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.impl.index.collector.DocValuesAccess;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.labelscan.bitmaps.BitmapExtractor;
import org.neo4j.kernel.api.impl.labelscan.bitmaps.LongPageIterator;

class PageOfRangesIterator extends PrefetchingIterator<PrimitiveLongIterator>
{
    private IndexSearcher searcher;
    private final Query query;
    private final BitmapDocumentFormat format;
    private final int rangesPerPage;
    private final int[] labels;
    private DocValuesCollector.LongValuesIterator rangesIterator;

    PageOfRangesIterator( BitmapDocumentFormat format, IndexSearcher searcher, int rangesPerPage, Query query,
            int... labels )
    {
        this.searcher = searcher;
        this.query = query;
        this.format = format;
        this.rangesPerPage = rangesPerPage;
        this.labels = labels;
        if (labels.length == 0)
        {
            throw new IllegalArgumentException( "At least one label required" );
        }
    }

    @Override
    protected PrimitiveLongIterator fetchNextOrNull()
    {
        if ( searcher == null )
        {
            return null; // we are done searching with this iterator
        }

        DocValuesCollector.LongValuesIterator ranges = getRanges();
        int pageSize = Math.min( ranges.remaining(), rangesPerPage );
        long[] rangeMap = new long[pageSize * 2];

        for ( int i = 0; i < pageSize; i++ )
        {
            long range = ranges.next();
            rangeMap[i * 2] = range;
            rangeMap[i * 2 + 1] = labeledBitmap( ranges );
        }

        if ( pageSize < rangesPerPage ) // not a full page => this is the last page (optimization)
        {
            searcher = null; // avoid searching again
        }
        return new LongPageIterator( new BitmapExtractor( format.bitmapFormat(), rangeMap ) );
    }

    private DocValuesCollector.LongValuesIterator getRanges() {
        if ( rangesIterator != null )
        {
            return rangesIterator;
        }
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector();
            searcher.search( query, docValuesCollector );
            rangesIterator = docValuesCollector.getValuesIterator( BitmapDocumentFormat.RANGE );
            return rangesIterator;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private long labeledBitmap( DocValuesAccess doc )
    {
        long bitmap = -1;
        for ( int label : labels )
        {
            bitmap &= doc.getValue( format.label( label ) );
        }
        return bitmap;
    }
}
