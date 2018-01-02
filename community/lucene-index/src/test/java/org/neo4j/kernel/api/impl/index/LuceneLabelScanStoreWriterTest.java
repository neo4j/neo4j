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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;

import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import static java.lang.Long.parseLong;
import static java.lang.String.valueOf;

import static org.neo4j.kernel.api.impl.index.BitmapDocumentFormat.RANGE;

public class LuceneLabelScanStoreWriterTest
{
    public static final BitmapDocumentFormat FORMAT = BitmapDocumentFormat._32;

    @Test
    public void shouldComplainIfNodesSuppliedOutOfRangeOrder() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 33;

        long node1Range = FORMAT.bitmapFormat().rangeOf( nodeId1 );
        long node2Range = FORMAT.bitmapFormat().rangeOf( nodeId2 );
        assertNotEquals( node1Range, node2Range );

        // when
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( new StubStorageService(), FORMAT, mock( Lock.class ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{} ) );
        try
        {
            writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{} ) );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsAndLabelsInIt() throws Exception
    {
        // given
        int nodeId = 1;
        int label1 = 201;
        int label2 = 202;
        StubStorageService storage = new StubStorageService();

        // when
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, FORMAT, mock( Lock.class ) );

        writer.write( NodeLabelUpdate.labelChanges( nodeId, new long[]{}, new long[]{label1, label2} ) );
        writer.close();

        // then
        long range = FORMAT.bitmapFormat().rangeOf( nodeId );
        Document document = storage.getDocument( new Term( RANGE, valueOf( range ) ) );

        assertTrue( FORMAT.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ),
                nodeId ) );
        assertTrue( FORMAT.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId ) );
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsInTheSameRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 3;
        int label1 = 201;
        int label2 = 202;
        StubStorageService storage = new StubStorageService();
        BitmapDocumentFormat format = BitmapDocumentFormat._32;
        long range = format.bitmapFormat().rangeOf( nodeId1 );
        assertEquals( range, format.bitmapFormat().rangeOf( nodeId2 ) );

        // when
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document = storage.getDocument( new Term( RANGE, valueOf( range ) ) );

        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ), nodeId1 ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    @Test
    public void shouldStoreDocumentWithNodeIdsInADifferentRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 33;
        int label1 = 201;
        int label2 = 202;
        StubStorageService storage = new StubStorageService();
        BitmapDocumentFormat format = BitmapDocumentFormat._32;

        long node1Range = format.bitmapFormat().rangeOf( nodeId1 );
        long node2Range = format.bitmapFormat().rangeOf( nodeId2 );
        assertNotEquals( node1Range, node2Range );

        // when
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document1 = storage.getDocument( new Term( RANGE, valueOf( node1Range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document1.get( valueOf( label1 ) ) ), nodeId1 ) );

        Document document2 = storage.getDocument( new Term( RANGE, valueOf( node2Range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document2.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    @Test
    public void shouldUpdateExistingDocumentWithNodesInTheSameRange() throws Exception
    {
        // given
        int nodeId1 = 1;
        int nodeId2 = 3;
        int label1 = 201;
        int label2 = 202;
        StubStorageService storage = new StubStorageService();
        BitmapDocumentFormat format = BitmapDocumentFormat._32;

        long range = format.bitmapFormat().rangeOf( nodeId1 );
        assertEquals( range, format.bitmapFormat().rangeOf( nodeId2 ) );

        // node already indexed
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId1, new long[]{}, new long[]{label1} ) );
        writer.close();

        // when
        writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );
        writer.write( NodeLabelUpdate.labelChanges( nodeId2, new long[]{}, new long[]{label2} ) );
        writer.close();

        // then
        Document document = storage.getDocument( new Term( RANGE, valueOf( range ) ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label1 ) ) ), nodeId1 ) );
        assertTrue( format.bitmapFormat().hasLabel( parseLong( document.get( valueOf( label2 ) ) ), nodeId2 ) );
    }

    private class StubStorageService implements LabelScanStorageStrategy.StorageService
    {
        private final Map<Term, Document> storage = new HashMap<>();

        @Override
        public void updateDocument( Term term, Document document ) throws IOException
        {
            storage.put( term, document );
        }

        @Override
        public void deleteDocuments( Term term ) throws IOException
        {
            storage.remove( term );
        }

        @Override
        public IndexSearcher acquireSearcher()
        {
            return new IndexSearcher( mock( IndexReader.class ) )
            {
                private final Map<Integer, Document> docIds = new HashMap<>();

                @Override
                public TopDocs search( Query query, int n )
                {
                    Document document = storage.get( ((TermQuery) query).getTerm() );
                    if ( document == null )
                    {
                        return new TopDocs( 0, new ScoreDoc[0], 0 );
                    }

                    int docId = new Random().nextInt();
                    docIds.put( docId, document );
                    int score = 10;
                    return new TopDocs( 1, new ScoreDoc[]{new ScoreDoc( docId, score )}, score );
                }

                @Override
                public Document doc( int docID )
                {
                    return docIds.get( docID );
                }
            };
        }

        @Override
        public void releaseSearcher( IndexSearcher searcher ) throws IOException
        {
        }

        @Override
        public void refreshSearcher() throws IOException
        {
        }

        public Document getDocument( Term term )
        {
            return storage.get( term );
        }
    }
}
