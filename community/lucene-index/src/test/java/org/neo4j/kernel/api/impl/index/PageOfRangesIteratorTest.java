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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.MapUtil;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
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
@RunWith( Parameterized.class )
public class PageOfRangesIteratorTest
{
    @Parameterized.Parameters( name = "{0} bits" )
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

        NumericDocValues rangeNDV = mock( NumericDocValues.class );
        when( rangeNDV.get( 11 ) ).thenReturn( 0x1L );
        when( rangeNDV.get( 16 ) ).thenReturn( 0x2L );
        when( rangeNDV.get( 37 ) ).thenReturn( 0x3L );

        NumericDocValues labelNDV = mock( NumericDocValues.class );
        when( labelNDV.get( 11 ) ).thenReturn( 0x01L );
        when( labelNDV.get( 16 ) ).thenReturn( 0x03L );
        when( labelNDV.get( 37 ) ).thenReturn( 0x30L );

        Map<String,NumericDocValues> docValues =
                MapUtil.genericMap( "range", rangeNDV, "7", labelNDV );
        IndexReaderStub reader = new IndexReaderStub( docValues );
        reader.setElements( new String[]{"11", "16", "37"} );
        final LeafReaderContext context = reader.getContext();

        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                DocValuesCollector collector = (DocValuesCollector) invocation.getArguments()[1];
                collector.doSetNextReader( context );
                collector.collect( 11 );
                collector.collect( 16 );
                collector.collect( 37 );
                return null;
            }
        } ).when( searcher ).search( same( query ), any( DocValuesCollector.class ) );

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

        verify( searcher, times( 1 ) ).search( same( query ), any( DocValuesCollector.class ) );
        verify( rangeNDV, times( 3 ) ).get( anyInt() );
        verify( labelNDV, times( 3 ) ).get( anyInt() );

        verifyNoMoreInteractions( searcher );
        verifyNoMoreInteractions( labelNDV );
        verifyNoMoreInteractions( rangeNDV );
    }

    static TopDocs docs( ScoreDoc... docs )
    {
        return new TopDocs( docs.length, docs, 0.0f );
    }

    static Document document( IndexableField... fields )
    {
        Document document = new Document();
        for ( IndexableField field : fields )
        {
            document.add( field );
        }
        return document;
    }
}
