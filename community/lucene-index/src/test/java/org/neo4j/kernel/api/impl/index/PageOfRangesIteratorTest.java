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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.concat;
import static org.neo4j.helpers.collection.IteratorUtil.primitivesList;

/**
 * Tests for reading through a {@link NodeRangeDocumentLabelScanStorageStrategy}.
 *
 * @see NodeRangeDocumentLabelScanStorageStrategyTest for tests for writing.
 */
@RunWith(Parameterized.class)
public class PageOfRangesIteratorTest
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

    public PageOfRangesIteratorTest( BitmapDocumentFormat format )
    {
        this.format = format;
    }

    @Test
    public void shouldReadPagesOfDocumentsFromSearcher() throws Exception
    {
        final int labelId = 7;
        final int pageSize = 2;
        // given
        Query query = mock( Query.class );
        IndexSearcher searcher = mock( IndexSearcher.class );
        ScoreDoc doc1 = new ScoreDoc( 37, 0.0f );
        ScoreDoc doc2 = new ScoreDoc( 16, 0.0f );
        ScoreDoc doc3 = new ScoreDoc( 11, 0.0f );
        when( searcher.searchAfter( any( ScoreDoc.class ), same( query ), anyInt() ) ).thenReturn(
                docs( doc1, doc2 ), // page1
                docs( doc3 )        // page2
        );
        when( searcher.doc( 37 ) ).thenReturn( document( format.rangeField( 0x1 ),
                                                         format.labelField( labelId, 0x01 ) ) );
        when( searcher.doc( 16 ) ).thenReturn( document( format.rangeField( 0x2 ),
                                                         format.labelField( labelId, 0x03 ) ) );
        when( searcher.doc( 11 ) ).thenReturn( document( format.rangeField( 0x3 ),
                                                         format.labelField( labelId, 0x30 ) ) );

        PrimitiveLongIterator iterator = concat(
                new PageOfRangesIterator( format, searcher, pageSize, query, labelId ) );

        // when
        List<Long> longs = primitivesList( iterator );

        // then
        assertEquals( asList(
        /*doc1:*/(1L << format.bitmapFormat().shift),
        /*doc2:*/(2L << format.bitmapFormat().shift), (2L << format.bitmapFormat().shift) + 1,
        /*doc3:*/(3L << format.bitmapFormat().shift) + 4, (3L << format.bitmapFormat().shift) + 5 ),
                      longs );

        ArgumentCaptor<ScoreDoc> prefixCollector = ArgumentCaptor.forClass( ScoreDoc.class );
        verify( searcher, times( 2 ) ).searchAfter( prefixCollector.capture(), same( query ), eq( 2 ) );
        assertEquals( asList( null, doc2 ), prefixCollector.getAllValues() );

        verify( searcher, times( 3 ) ).doc( anyInt() );
        verifyNoMoreInteractions( searcher );
    }

    static TopDocs docs( ScoreDoc... docs )
    {
        return new TopDocs( docs.length, docs, 0.0f );
    }

    static Document document( Fieldable... fields )
    {
        Document document = new Document();
        for ( Fieldable field : fields )
        {
            document.add( field );
        }
        return document;
    }
}
