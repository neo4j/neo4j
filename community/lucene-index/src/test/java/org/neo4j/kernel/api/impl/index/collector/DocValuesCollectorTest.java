/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index.collector;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.api.impl.index.IndexReaderStub;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public final class DocValuesCollectorTest
{
    @Test
    public void shouldStartWithEmptyMatchingDocs()
    {
        //given
        DocValuesCollector collector = new DocValuesCollector();

        // when
        // then
        assertEquals( emptyList(), collector.getMatchingDocs() );
    }

    @Test
    public void shouldCollectAllHitsPerSegment() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector();
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 3 );
        collector.collect( 5 );
        collector.collect( 9 );

        // then
        assertEquals( 4, collector.getTotalHits() );
        List<DocValuesCollector.MatchingDocs> allMatchingDocs = collector.getMatchingDocs();
        assertEquals( 1, allMatchingDocs.size() );
        DocValuesCollector.MatchingDocs matchingDocs = allMatchingDocs.get( 0 );
        assertSame( readerStub.getContext(), matchingDocs.context );
        assertEquals( 4, matchingDocs.totalHits );
        DocIdSetIterator idIterator = matchingDocs.docIdSet.iterator();
        assertEquals( 1, idIterator.nextDoc() );
        assertEquals( 3, idIterator.nextDoc() );
        assertEquals( 5, idIterator.nextDoc() );
        assertEquals( 9, idIterator.nextDoc() );
        assertEquals( DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc() );
    }

    @Test
    public void shouldCollectOneMatchingDocsPerSegment() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector();
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 3 );
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 5 );
        collector.collect( 9 );

        // then
        assertEquals( 4, collector.getTotalHits() );
        List<DocValuesCollector.MatchingDocs> allMatchingDocs = collector.getMatchingDocs();
        assertEquals( 2, allMatchingDocs.size() );

        DocValuesCollector.MatchingDocs matchingDocs = allMatchingDocs.get( 0 );
        assertSame( readerStub.getContext(), matchingDocs.context );
        assertEquals( 2, matchingDocs.totalHits );
        DocIdSetIterator idIterator = matchingDocs.docIdSet.iterator();
        assertEquals( 1, idIterator.nextDoc() );
        assertEquals( 3, idIterator.nextDoc() );
        assertEquals( DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc() );

        matchingDocs = allMatchingDocs.get( 1 );
        assertSame( readerStub.getContext(), matchingDocs.context );
        assertEquals( 2, matchingDocs.totalHits );
        idIterator = matchingDocs.docIdSet.iterator();
        assertEquals( 5, idIterator.nextDoc() );
        assertEquals( 9, idIterator.nextDoc() );
        assertEquals( DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc() );
    }

    @Test
    public void shouldNotSaveScoresWhenNotRequired() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );

        // then
        DocValuesCollector.MatchingDocs matchingDocs = collector.getMatchingDocs().get( 0 );
        assertNull( matchingDocs.scores );
    }

    @Test
    public void shouldSaveScoresWhenRequired() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 13.42f ) );
        collector.collect( 1 );

        // then
        DocValuesCollector.MatchingDocs matchingDocs = collector.getMatchingDocs().get( 0 );
        assertArrayEquals( new float[]{13.42f}, matchingDocs.scores, 0.0f );
    }

    @Test
    public void shouldSaveScoresInADenseArray() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 1.0f ) );
        collector.collect( 1 );
        collector.setScorer( constantScorer( 41.0f ) );
        collector.collect( 41 );

        // then
        DocValuesCollector.MatchingDocs matchingDocs = collector.getMatchingDocs().get( 0 );
        assertArrayEquals( new float[]{1.0f, 41.0f}, matchingDocs.scores, 0.0f );
    }

    @Test
    public void shouldDynamicallyResizeScoresArray() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 1.0f ) );
        // scores starts with array size of 32, adding 42 docs forces resize
        for ( int i = 0; i < 42; i++ )
        {
            collector.collect( i );
        }

        // then
        DocValuesCollector.MatchingDocs matchingDocs = collector.getMatchingDocs().get( 0 );
        float[] scores = new float[42];
        Arrays.fill( scores, 1.0f );
        assertArrayEquals( scores, matchingDocs.scores, 0.0f );
    }

    @Test
    public void shouldReturnIndexHitsInIndexOrderWhenNoSortIsGiven() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector();
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 2 );

        // then
        IndexHits<Document> indexHits = collector.getIndexHits( null );
        assertEquals( 2, indexHits.size() );
        assertEquals( "1", indexHits.next().get( "id" ) );
        assertEquals( "2", indexHits.next().get( "id" ) );
        assertFalse( indexHits.hasNext() );
    }

    @Test
    public void shouldReturnIndexHitsOrderedByRelevance() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 1.0f ) );
        collector.collect( 1 );
        collector.setScorer( constantScorer( 2.0f ) );
        collector.collect( 2 );

        // then
        IndexHits<Document> indexHits = collector.getIndexHits( Sort.RELEVANCE );
        assertEquals( 2, indexHits.size() );
        assertEquals( "2", indexHits.next().get( "id" ) );
        assertEquals( 2.0f, indexHits.currentScore(), 0.0f );
        assertEquals( "1", indexHits.next().get( "id" ) );
        assertEquals( 1.0f, indexHits.currentScore(), 0.0f );
        assertFalse( indexHits.hasNext() );
    }

    @Test
    public void shouldReturnIndexHitsInGivenSortOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 43 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 3 );
        collector.collect( 37 );
        collector.collect( 42 );

        // then
        Sort byIdDescending = new Sort( new SortField( "id", SortField.Type.LONG, true ) );
        IndexHits<Document> indexHits = collector.getIndexHits( byIdDescending );
        assertEquals( 4, indexHits.size() );
        assertEquals( "42", indexHits.next().get( "id" ) );
        assertEquals( "37", indexHits.next().get( "id" ) );
        assertEquals( "3", indexHits.next().get( "id" ) );
        assertEquals( "1", indexHits.next().get( "id" ) );
        assertFalse( indexHits.hasNext() );
    }

    @Test
    public void shouldSilentlyMergeAllSegments() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 2 );

        // then
        IndexHits<Document> indexHits = collector.getIndexHits( null );
        assertEquals( 2, indexHits.size() );
        assertEquals( "1", indexHits.next().get( "id" ) );
        assertEquals( "2", indexHits.next().get( "id" ) );
        assertFalse( indexHits.hasNext() );
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoHits() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );

        // then
        IndexHits<Document> indexHits = collector.getIndexHits( null );
        assertEquals( 0, indexHits.size() );
        assertEquals( Float.NaN, indexHits.currentScore(), 0.0f );
        assertFalse( indexHits.hasNext() );
    }

    @Test
    public void shouldReadDocValuesInIndexOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 2 );

        // then
        DocValuesCollector.LongValuesIterator valuesIterator = collector.getValuesIterator( "id" );
        assertEquals( 2, valuesIterator.remaining() );
        assertEquals( 1, valuesIterator.next() );
        assertEquals( 2, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldSilentlyMergeSegmentsWhenReadingDocValues() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 2 );

        // then
        DocValuesCollector.LongValuesIterator valuesIterator = collector.getValuesIterator( "id" );
        assertEquals( 2, valuesIterator.remaining() );
        assertEquals( 1, valuesIterator.next() );
        assertEquals( 2, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoDocValues()
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );

        // then
        DocValuesCollector.LongValuesIterator valuesIterator = collector.getValuesIterator( "id" );
        assertEquals( 0, valuesIterator.remaining() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldReturnDocValuesInIndexOrderWhenNoSortIsGiven() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 2 );

        // then
        PrimitiveLongIterator valuesIterator = collector.getSortedValuesIterator( "id", null );
        assertEquals( 1, valuesIterator.next() );
        assertEquals( 2, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldReturnDocValuesInRelevanceOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 1.0f ) );
        collector.collect( 1 );
        collector.setScorer( constantScorer( 2.0f ) );
        collector.collect( 2 );

        // then
        PrimitiveLongIterator valuesIterator = collector.getSortedValuesIterator( "id", Sort.RELEVANCE );
        assertEquals( 2, valuesIterator.next() );
        assertEquals( 1, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldReturnDocValuesInGivenOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.collect( 1 );
        collector.collect( 2 );

        // then
        Sort byIdDescending = new Sort( new SortField( "id", SortField.Type.LONG, true ) );
        PrimitiveLongIterator valuesIterator = collector.getSortedValuesIterator( "id", byIdDescending );
        assertEquals( 2, valuesIterator.next() );
        assertEquals( 1, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldSilentlyMergeSegmentsWhenReturnDocValuesInOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( true );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 1.0f ) );
        collector.collect( 1 );
        collector.doSetNextReader( readerStub.getContext() );
        collector.setScorer( constantScorer( 2.0f ) );
        collector.collect( 2 );

        // then
        PrimitiveLongIterator valuesIterator = collector.getSortedValuesIterator( "id", Sort.RELEVANCE );
        assertEquals( 2, valuesIterator.next() );
        assertEquals( 1, valuesIterator.next() );
        assertFalse( valuesIterator.hasNext() );
    }

    @Test
    public void shouldReturnEmptyIteratorWhenNoDocValuesInOrder() throws Exception
    {
        // given
        DocValuesCollector collector = new DocValuesCollector( false );
        IndexReaderStub readerStub = indexReaderWithMaxDocs( 42 );

        // when
        collector.doSetNextReader( readerStub.getContext() );

        // then
        PrimitiveLongIterator valuesIterator = collector.getSortedValuesIterator( "id", Sort.RELEVANCE );
        assertFalse( valuesIterator.hasNext() );
    }

    private IndexReaderStub indexReaderWithMaxDocs( int maxDocs )
    {
        NumericDocValues identityValues = new NumericDocValues()
        {
            @Override
            public long get( int docID )
            {
                return docID;
            }
        };
        IndexReaderStub stub = new IndexReaderStub( identityValues );
        stub.setElements( new String[maxDocs] );
        return stub;
    }

    private Scorer constantScorer( float score )
    {
        return new ConstantScoreScorer( null, score, (DocIdSetIterator) null );
    }
}
