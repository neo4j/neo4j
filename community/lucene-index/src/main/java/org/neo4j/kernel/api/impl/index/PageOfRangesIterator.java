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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.impl.index.bitmaps.BitmapExtractor;
import org.neo4j.kernel.api.impl.index.bitmaps.LongPageIterator;

class PageOfRangesIterator extends PrefetchingIterator<PrimitiveLongIterator>
{
    private IndexSearcher searcher;
    private final Query query;
    private final BitmapDocumentFormat format;
    private final int rangesPerPage;
    private final int[] labels;
    private ScoreDoc lastDoc;

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
        try
        {
            TopDocs docs = searcher.searchAfter( lastDoc, query, rangesPerPage );
            lastDoc = null;
            int docCount = docs != null ? docs.scoreDocs.length : 0;
            if ( docCount == 0 )
            {
                searcher = null; // avoid searching again
                return null;
            }
            lastDoc = docs.scoreDocs[docCount - 1];
            long[] rangeMap = new long[docCount * 2];
            for ( int i = 0; i < docCount; i++ )
            {
                Document doc = searcher.doc( docs.scoreDocs[i].doc );
                rangeMap[i * 2] = format.rangeOf( doc );
                rangeMap[i * 2 + 1] = labeledBitmap( doc );
            }
            if ( docCount < rangesPerPage ) // not a full page => this is the last page (optimization)
            {
                searcher = null; // avoid searching again
            }
            return new LongPageIterator( new BitmapExtractor( format.bitmapFormat(), rangeMap ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e ); // TODO: something better?
        }
    }

    private long labeledBitmap( Document doc )
    {
        long bitmap = -1;
        for ( int label : labels )
        {
            bitmap &= format.mapOf( doc, label );
        }
        return bitmap;
    }
}
