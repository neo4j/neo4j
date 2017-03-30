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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.impl.index.IndexReaderStub;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;
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

@RunWith( Parameterized.class )
public class PageOfRangesIteratorTest
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

        Map<String,NumericDocValues> docValues = MapUtil.genericMap( "range", rangeNDV, "7", labelNDV );
        IndexReaderStub reader = new IndexReaderStub( docValues );
        reader.setElements( new String[]{"11", "16", "37"} );
        final LeafReaderContext context = reader.getContext();

        doAnswer( invocation ->
        {
            DocValuesCollector collector = (DocValuesCollector) invocation.getArguments()[1];
            collector.doSetNextReader( context );
            collector.collect( 11 );
            collector.collect( 16 );
            collector.collect( 37 );
            return null;
        } ).when( searcher ).search( same( query ), any( DocValuesCollector.class ) );

        PrimitiveLongIterator iterator = concat(
                new PageOfRangesIterator( format, searcher, pageSize, query, Occur.MUST, labelId ) );

        // when
        List<Long> longs = PrimitiveLongCollections.asList( iterator );

        // then
        assertEquals( asList(
        /*doc1:*/1L << format.bitmapFormat().shift,
        /*doc2:*/2L << format.bitmapFormat().shift, (2L << format.bitmapFormat().shift) + 1,
        /*doc3:*/(3L << format.bitmapFormat().shift) + 4, (3L << format.bitmapFormat().shift) + 5 ),
                longs );

        verify( searcher, times( 1 ) ).search( same( query ), any( DocValuesCollector.class ) );
        verify( rangeNDV, times( 6 ) ).get( anyInt() );
        verify( labelNDV, times( 3 ) ).get( anyInt() );

        verifyNoMoreInteractions( searcher );
        verifyNoMoreInteractions( labelNDV );
        verifyNoMoreInteractions( rangeNDV );
    }
}
