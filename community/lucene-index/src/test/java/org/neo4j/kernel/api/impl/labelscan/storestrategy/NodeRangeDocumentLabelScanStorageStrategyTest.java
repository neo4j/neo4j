/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.impl.index.collector.FirstHitCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartition;
import org.neo4j.kernel.api.impl.labelscan.WritableDatabaseLabelScanIndex;
import org.neo4j.kernel.api.impl.labelscan.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.labelscan.writer.PartitionedLuceneLabelScanWriter;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

/**
 * Tests updating the label scan store through a {@link NodeRangeDocumentLabelScanStorageStrategy}.
 * The tests for reading through that strategy are in {@link PageOfRangesIteratorTest}, since the bulk of the
 * implementation of reading is in {@link PageOfRangesIterator}.
 */
@RunWith( Parameterized.class )
public class NodeRangeDocumentLabelScanStorageStrategyTest
{
    @Parameter
    public BitmapDocumentFormat format;

    @Parameters( name = "{0} bits" )
    public static List<Object[]> formats()
    {
        return Stream.of( BitmapDocumentFormat.values() )
                .map( format -> new Object[]{format} )
                .collect( toList() );
    }

    @Test
    public void shouldCreateNewDocumentsForNewlyLabeledNodes() throws Exception
    {
        // given
        WritableIndexPartition partition = mock( WritableIndexPartition.class );
        WritableDatabaseLabelScanIndex index = buildLuceneIndex( partition );

        PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
        when( partition.acquireSearcher() ).thenReturn( partitionSearcher );

        IndexWriter indexWriter = mock( IndexWriter.class );
        when( partition.getIndexWriter() ).thenReturn( indexWriter );

        IndexSearcher searcher = mock( IndexSearcher.class );
        when( partitionSearcher.getIndexSearcher() ).thenReturn( searcher );
        when( searcher.search( new TermQuery( format.rangeTerm( 0 ) ), 1 ) ).thenReturn( emptyTopDocs() );
        when( searcher.search( new TermQuery( format.rangeTerm( 1 ) ), 1 ) ).thenReturn( null );

        LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

        // when
        writer.write( labelChanges( 0, labels(), labels( 6, 7 ) ) );
        writer.write( labelChanges( 1, labels(), labels( 6, 8 ) ) );
        writer.write( labelChanges( 1 << format.bitmapFormat().shift, labels(), labels( 7 ) ) );
        writer.close();

        // then
        verify( partition, times( 2 ) ).acquireSearcher();
        verify( partitionSearcher, times( 2 ) ).getIndexSearcher();
        verify( partition, times( 2 ) ).getIndexWriter();
        verify( partitionSearcher, times( 2 ) ).close();

        verify( indexWriter ).updateDocument(
                eq( format.rangeTerm( 0 ) ),
                match( document( format.rangeField( 0 ),
                        format.labelField( 6, 0x3 ),
                        format.labelField( 7, 0x1 ),
                        format.labelField( 8, 0x2 ),
                        format.labelSearchField( 8 ) ) ) );

        verify( indexWriter ).updateDocument(
                eq( format.rangeTerm( 1 ) ),
                match( document( format.rangeField( 1 ),
                        format.labelField( 7, 0x1 ),
                        format.labelSearchField( 7 ) ) ) );

        verify( index ).maybeRefreshBlocking();
        verifyNoMoreInteractions( partition );
    }

    @Test
    public void shouldUpdateDocumentsForReLabeledNodes() throws Exception
    {
        // given
        Document givenDoc = new Document();
        format.addRangeValuesField( givenDoc, 0 );
        format.addLabelFields( givenDoc, "7", 0x70L );

        WritableDatabaseLabelScanIndex index = mock( WritableDatabaseLabelScanIndex.class );
        IndexWriter indexWriter = mock( IndexWriter.class );
        WritableIndexPartition partition = newIndexPartitionMock( indexWriter, givenDoc );
        when( index.getPartitions() ).thenReturn( Collections.singletonList( partition ) );

        LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

        // when
        writer.write( labelChanges( 0, labels(), labels( 7, 8 ) ) );
        writer.close();

        // then
        Document thenDoc = new Document();
        format.addRangeValuesField( thenDoc, 0 );
        format.addLabelFields( thenDoc, "7", 0x71L );
        format.addLabelAndSearchFields( thenDoc, 8, new Bitmap( 0x01L ) );
        verify( indexWriter ).updateDocument( eq( format.rangeTerm( 0 ) ), match( thenDoc ) );
    }

    @Test
    public void shouldRemoveLabelFieldsThatDoesNotRepresentAnyNodes() throws Exception
    {
        // given
        IndexWriter indexWriter = mock( IndexWriter.class );
        Document doc = document( format.rangeField( 0 ), format.labelField( 7, 0x1 ), format.labelField( 8, 0x1 ) );
        WritableIndexPartition partition = newIndexPartitionMock( indexWriter, doc );

        WritableDatabaseLabelScanIndex index = buildLuceneIndex( partition );

        LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

        // when
        writer.write( labelChanges( 0, labels( 7, 8 ), labels( 8 ) ) );
        writer.close();

        // then
        verify( indexWriter ).updateDocument( eq( format.rangeTerm( 0 ) ),
                match( document( format.rangeField( 0 ),
                        format.labelField( 8, 0x01 ),
                        format.labelSearchField( 8 ) ) ) );
    }

    @Test
    public void shouldDeleteEmptyDocuments() throws Exception
    {
        // given
        IndexWriter indexWriter = mock( IndexWriter.class );
        Document doc = document( format.rangeField( 0 ), format.labelField( 7, 0x1 ) );
        WritableIndexPartition partition = newIndexPartitionMock( indexWriter, doc );

        WritableDatabaseLabelScanIndex index = buildLuceneIndex( partition );

        LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

        // when
        writer.write( labelChanges( 0, labels( 7 ), labels() ) );
        writer.close();

        // then
        verify( indexWriter ).deleteDocuments( format.rangeTerm( 0 ) );
    }

    @Test
    public void shouldUpdateDocumentToReflectLabelsAfterRegardlessOfPreviousContent() throws Exception
    {
        // given
        IndexWriter indexWriter = mock( IndexWriter.class );
        Document doc = document( format.rangeField( 0 ), format.labelField( 6, 0x1 ), format.labelField( 7, 0x1 ) );
        WritableIndexPartition partition = newIndexPartitionMock( indexWriter, doc );

        WritableDatabaseLabelScanIndex index = buildLuceneIndex( partition );

        LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

        // when
        writer.write( labelChanges( 0, labels( 7 ), labels( 7, 8 ) ) );
        writer.close();

        // then
        verify( indexWriter ).updateDocument(
                eq( format.rangeTerm( 0 ) ),
                match( document( format.rangeField( 0 ),
                        format.labelField( 7, 0x01 ),
                        format.labelField( 8, 0x01 ),
                        format.labelSearchField( 7 ),
                        format.labelSearchField( 8 ) ) ) );
    }

    @Test
    public void shouldStoreAnyNodeIdInRange() throws Exception
    {
        for ( int i = 0, max = 1 << format.bitmapFormat().shift; i < max; i++ )
        {
            // given
            IndexWriter indexWriter = mock( IndexWriter.class );
            WritableIndexPartition partition = newIndexPartitionMock( indexWriter );

            WritableDatabaseLabelScanIndex index = buildLuceneIndex( partition );

            LabelScanWriter writer = new PartitionedLuceneLabelScanWriter( index, format );

            // when
            writer.write( labelChanges( i, labels(), labels( 7 ) ) );
            writer.close();

            // then
            Document document = new Document();
            format.addRangeValuesField( document, 0 );
            format.addLabelAndSearchFields( document, 7, new Bitmap( 1L << i ) );
            verify( indexWriter ).updateDocument( eq( format.rangeTerm( 0 ) ), match( document ) );
        }
    }

    private WritableIndexPartition newIndexPartitionMock( IndexWriter indexWriter, Document... documents )
            throws IOException
    {
        WritableIndexPartition partition = mock( WritableIndexPartition.class );

        PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
        when( partition.acquireSearcher() ).thenReturn( partitionSearcher );

        when( partition.getIndexWriter() ).thenReturn( indexWriter );

        IndexSearcher searcher = mock( IndexSearcher.class );
        when( partitionSearcher.getIndexSearcher() ).thenReturn( searcher );

        for ( int i = 0; i < documents.length; i++ )
        {
            int docId = i;
            doAnswer( invocation ->
            {
                FirstHitCollector collector = (FirstHitCollector) invocation.getArguments()[1];
                try
                {
                    collector.collect( docId );
                }
                catch ( CollectionTerminatedException swallow )
                {
                    // swallow
                }
                return null;
            } ).when( searcher ).search(
                    eq( new TermQuery( format.rangeTerm( documents[i] ) ) ),
                    any( FirstHitCollector.class )
            );
            when( searcher.doc( i ) ).thenReturn( documents[i] );
        }

        return partition;
    }

    private static long[] labels( long... labels )
    {
        return labels;
    }

    private static Document document( IndexableField... fields )
    {
        Document document = new Document();
        Stream.of( fields ).forEach( document::add );
        return document;
    }

    private static TopDocs emptyTopDocs()
    {
        return new TopDocs( 0, new ScoreDoc[0], 0.0f );
    }

    private static Document match( Document document )
    {
        return argThat( new TypeSafeMatcher<Document>()
        {
            @Override
            protected boolean matchesSafely( Document item )
            {
                return equal( fields( document ), fields( item ) );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( document );
            }

            private Map<String,IndexableField> fields( Document doc )
            {
                Map<String,IndexableField> these = new HashMap<>();
                for ( IndexableField field : doc.getFields() )
                {
                    these.put( field.name(), field );
                }
                return these;
            }

            boolean equal( Map<String,IndexableField> these, Map<String,IndexableField> those )
            {
                if ( !these.keySet().equals( those.keySet() ) )
                {
                    return false;
                }
                for ( Map.Entry<String,IndexableField> entry : these.entrySet() )
                {
                    if ( !equal( entry.getValue(), those.get( entry.getKey() ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            boolean equal( IndexableField lhs, IndexableField rhs )
            {
                if ( lhs.binaryValue() != null && rhs.binaryValue() != null )
                {
                    return Arrays.equals( lhs.binaryValue().bytes, rhs.binaryValue().bytes );
                }
                return lhs.stringValue().equals( rhs.stringValue() );
            }
        } );
    }

    private WritableDatabaseLabelScanIndex buildLuceneIndex( WritableIndexPartition partition )
    {
        WritableDatabaseLabelScanIndex index = mock( WritableDatabaseLabelScanIndex.class );
        when( index.getPartitions() ).thenReturn( singletonList( partition ) );
        return index;
    }
}
