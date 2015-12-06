/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.index.PageOfRangesIteratorTest.docs;
import static org.neo4j.kernel.api.impl.index.PageOfRangesIteratorTest.document;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

/**
 * Tests updating the label scan store through a {@link NodeRangeDocumentLabelScanStorageStrategy}.
 * The tests for reading through that strategy are in {@link PageOfRangesIteratorTest}, since the bulk of the
 * implementation of reading is in {@link PageOfRangesIterator}.
 */
@RunWith(Parameterized.class)
public class NodeRangeDocumentLabelScanStorageStrategyTest
{
    @Parameterized.Parameters(name = "{0} bits")
    public static List<Object[]> formats()
    {
        ArrayList<Object[]> parameters = new ArrayList<>();
        for ( BitmapDocumentFormat format : BitmapDocumentFormat.values() )
        {
            parameters.add( new Object[]{format} );
        }
        return parameters;
    }

    private final BitmapDocumentFormat format;

    public NodeRangeDocumentLabelScanStorageStrategyTest( BitmapDocumentFormat format )
    {
        this.format = format;
    }

    @Test
    public void shouldCreateNewDocumentsForNewlyLabeledNodes() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = mock( LabelScanStorageStrategy.StorageService.class );

        IndexSearcher searcher = mock( IndexSearcher.class );
        when( storage.acquireSearcher() ).thenReturn( searcher );
        when( searcher.search( new TermQuery( format.rangeTerm( 0 ) ), 1 ) ).thenReturn( docs() );
        when( searcher.search( new TermQuery( format.rangeTerm( 1 ) ), 1 ) ).thenReturn( null );

        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        // when
        writer.write( labelChanges( 0, labels(), labels( 6, 7 ) ) );
        writer.write( labelChanges( 1, labels(), labels( 6, 8 ) ) );
        writer.write( labelChanges( 1 << format.bitmapFormat().shift, labels(), labels( 7 ) ) );
        writer.close();

        // then
        verify( storage ).acquireSearcher();
        verify( storage ).releaseSearcher( searcher );
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 6, 0x3 ),
                                                           format.labelField( 7, 0x1 ),
                                                           format.labelField( 8, 0x2 ),
                                                           format.labelSearchField( 8 ) ) ) );
        verify( storage ).updateDocument( eq( format.rangeTerm( 1 ) ),
                                          match( document( format.rangeField( 1 ),
                                                           format.labelField( 7, 0x1 ),
                                                           format.labelSearchField( 7 ) ) ) );
        verify( storage ).refreshSearcher();
        verifyNoMoreInteractions( storage );
    }

    @Test
    public void shouldUpdateDocumentsForReLabeledNodes() throws Exception
    {
        // given
        Document givenDoc = new Document();
        format.addRangeValuesField( givenDoc, 0 );
        format.addLabelFields( givenDoc, "7", 0x70L );
        LabelScanStorageStrategy.StorageService storage = storage(givenDoc);

        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        // when
        writer.write( labelChanges( 0, labels(), labels( 7, 8 ) ) );
        writer.close();

        // then
        Document thenDoc = new Document();
        format.addRangeValuesField( thenDoc, 0 );
        format.addLabelFields( thenDoc, "7", 0x71L );
        format.addLabelAndSearchFields( thenDoc, 8, new Bitmap( 0x01L ) );
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ), match( thenDoc ) );
    }

    @Test
    public void shouldRemoveLabelFieldsThatDoesNotRepresentAnyNodes() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 7, 0x1 ),
                          format.labelField( 8, 0x1 ) ) );

        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        // when
        writer.write( labelChanges( 0, labels( 7, 8 ), labels( 8 ) ) );
        writer.close();

        // then
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 8, 0x01 ),
                                                           format.labelSearchField( 8 ) ) ) );
    }

    @Test
    public void shouldDeleteEmptyDocuments() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 7, 0x1 ) ) );

        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        // when
        writer.write( labelChanges( 0, labels( 7 ), labels() ) );
        writer.close();

        // then
        verify( storage ).deleteDocuments( format.rangeTerm( 0 ) );
    }

    @Test
    public void shouldUpdateDocumentToReflectLabelsAfterRegardlessOfPreviousContent() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 6, 0x1 ),
                          format.labelField( 7, 0x1 ) ) );

        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

        // when
        writer.write( labelChanges( 0, labels( 7 ), labels( 7, 8 ) ) );
        writer.close();

        // then
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
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
            LabelScanStorageStrategy.StorageService storage = storage();

            LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, mock( Lock.class ) );

            // when
            writer.write( labelChanges( i, labels(), labels( 7 ) ) );
            writer.close();

            // then
            Document document = new Document();
            format.addRangeValuesField( document, 0 );
            format.addLabelAndSearchFields( document, 7, new Bitmap( 1L << i ) );
            verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                              match( document ) );
        }
    }

    @Test
    public void shouldUnlockInClose() throws Exception
    {
        // GIVEN
        LabelScanStorageStrategy.StorageService storage = storage();
        Lock lock = mock( Lock.class );
        LuceneLabelScanWriter writer = new LuceneLabelScanWriter( storage, format, lock );

        // WHEN
        writer.close();

        // THEN
        verify( lock ).unlock();
    }

    private LabelScanStorageStrategy.StorageService storage( Document... documents ) throws Exception
    {
        LabelScanStorageStrategy.StorageService storage = mock( LabelScanStorageStrategy.StorageService.class );
        IndexSearcher searcher = mock( IndexSearcher.class );
        when( storage.acquireSearcher() ).thenReturn( searcher );
        for ( int i = 0; i < documents.length; i++ )
        {
            final int docId = i;
            doAnswer( invocation -> {
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
        return storage;
    }

    private static long[] labels( long... labels )
    {
        return labels;
    }

    private static Document match( final Document document )
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
                Map<String, IndexableField> these = new HashMap<>();
                for ( IndexableField field : doc.getFields() )
                {
                    these.put( field.name(), field );
                }
                return these;
            }

            boolean equal( Map<String, IndexableField> these, Map<String, IndexableField> those )
            {
                if ( !these.keySet().equals( those.keySet() ) )
                {
                    return false;
                }
                for ( Map.Entry<String, IndexableField> entry : these.entrySet() )
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
}
