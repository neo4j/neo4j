/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.kernel.api.impl.index.bitmaps.BitmapFormat;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;

import static java.util.Arrays.asList;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.impl.index.PageOfRangesIteratorTest.docs;
import static org.neo4j.kernel.api.impl.index.PageOfRangesIteratorTest.document;
import static org.neo4j.kernel.api.scan.NodeLabelUpdate.labelChanges;

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
        for ( BitmapFormat format : BitmapFormat.values() )
        {
            parameters.add( new Object[]{format} );
        }
        return parameters;
    }

    private final BitmapDocumentFormat format;

    public NodeRangeDocumentLabelScanStorageStrategyTest( BitmapFormat format )
    {
        this.format = new BitmapDocumentFormat( format );
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

        NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
        List<NodeLabelUpdate> updates = asList(
                labelChanges( 0, labels(), labels( 6, 7 ) ),
                labelChanges( 1, labels(), labels( 6, 8 ) ),
                labelChanges( 1 << format.bitmapFormat().shift, labels(), labels( 7 ) )
        );

        // when
        strategy.applyUpdates( storage, updates.iterator() );

        // then
        verify( storage ).acquireSearcher();
        verify( storage ).releaseSearcher( searcher );
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 6, 0x3 ),
                                                           format.labelField( 7, 0x1 ),
                                                           format.labelField( 8, 0x2 ) ) ) );
        verify( storage ).updateDocument( eq( format.rangeTerm( 1 ) ),
                                          match( document( format.rangeField( 1 ),
                                                           format.labelField( 7, 0x1 ) ) ) );
        verifyNoMoreInteractions( storage );
    }

    @Test
    public void shouldUpdateDocumentsForReLabeledNodes() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 7, 0x70 ) )
        );

        NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
        List<NodeLabelUpdate> updates = asList(
                labelChanges( 0, labels(), labels( 7, 8 ) )
        );

        // when
        strategy.applyUpdates( storage, updates.iterator() );

        // then
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 7, 0x71 ),
                                                           format.labelField( 8, 0x01 ) ) ) );
    }

    @Test
    public void shouldRemoveLabelFieldsThatDoesNotRepresentAnyNodes() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 7, 0x1 ),
                          format.labelField( 8, 0x1 ) ) );

        NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
        List<NodeLabelUpdate> updates = asList(
                labelChanges( 0, labels( 7, 8 ), labels( 8 ) )
        );

        // when
        strategy.applyUpdates( storage, updates.iterator() );

        // then
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 8, 0x01 ) ) ) );
    }

    @Test
    public void shouldDeleteEmptyDocuments() throws Exception
    {
        // given
        LabelScanStorageStrategy.StorageService storage = storage(
                document( format.rangeField( 0 ),
                          format.labelField( 7, 0x1 ) ) );

        NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
        List<NodeLabelUpdate> updates = asList(
                labelChanges( 0, labels( 7 ), labels() )
        );

        // when
        strategy.applyUpdates( storage, updates.iterator() );

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

        NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
        List<NodeLabelUpdate> updates = asList(
                labelChanges( 0, labels( 7 ), labels( 7, 8 ) )
        );

        // when
        strategy.applyUpdates( storage, updates.iterator() );

        // then
        verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                          match( document( format.rangeField( 0 ),
                                                           format.labelField( 7, 0x01 ),
                                                           format.labelField( 8, 0x01 ) ) ) );
    }

    @Test
    public void shouldStoreAnyNodeIdInRange() throws Exception
    {
        for ( int i = 0, max = 1 << format.bitmapFormat().shift; i < max; i++ )
        {
            // given
            LabelScanStorageStrategy.StorageService storage = storage();

            NodeRangeDocumentLabelScanStorageStrategy strategy = new NodeRangeDocumentLabelScanStorageStrategy(
                    format );
            List<NodeLabelUpdate> updates = asList( labelChanges( i, labels(), labels( 7 ) ) );

            // when
            strategy.applyUpdates( storage, updates.iterator() );

            // then
            verify( storage ).updateDocument( eq( format.rangeTerm( 0 ) ),
                                              match( document( format.rangeField( 0 ),
                                                               format.labelField( 7, 1L << i ) ) ) );
        }
    }

    private LabelScanStorageStrategy.StorageService storage( Document... documents ) throws Exception
    {
        LabelScanStorageStrategy.StorageService storage = mock( LabelScanStorageStrategy.StorageService.class );
        IndexSearcher searcher = mock( IndexSearcher.class );
        when( storage.acquireSearcher() ).thenReturn( searcher );
        for ( int i = 0; i < documents.length; i++ )
        {
            when( searcher.search( new TermQuery( format.rangeTerm( documents[i] ) ), 1 ) )
                    .thenReturn( docs( new ScoreDoc( i, 0.0f ) ) );
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

            private Map<String, Fieldable> fields( Document doc )
            {
                Map<String, Fieldable> these = new HashMap<>();
                for ( Fieldable field : doc.getFields() )
                {
                    these.put( field.name(), field );
                }
                return these;
            }

            boolean equal( Map<String, Fieldable> these, Map<String, Fieldable> those )
            {
                if ( !these.keySet().equals( those.keySet() ) )
                {
                    return false;
                }
                for ( Map.Entry<String, Fieldable> entry : these.entrySet() )
                {
                    if ( !equal( entry.getValue(), those.get( entry.getKey() ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            boolean equal( Fieldable lhs, Fieldable rhs )
            {
                if ( lhs.isBinary() && rhs.isBinary() )
                {
                    return Arrays.equals( lhs.getBinaryValue(), rhs.getBinaryValue() );
                }
                return lhs.stringValue().equals( rhs.stringValue() );
            }
        } );
    }
}
