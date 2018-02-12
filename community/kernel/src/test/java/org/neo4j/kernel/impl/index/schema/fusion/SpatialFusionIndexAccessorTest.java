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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.assertResultContainsAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.mockedAllEntriesReader;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.mockedAllEntriesReaderUnknownMaxCount;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class SpatialFusionIndexAccessorTest
{

    private SpatialFusionIndexAccessor fusionIndexAccessor;
    private Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap = new HashMap<>();

    @Before
    public void setup() throws Exception
    {
        SpatialKnownIndex.Factory indexFactory = mock( SpatialKnownIndex.Factory.class );

        for ( CoordinateReferenceSystem crs : asList( WGS84, Cartesian ) )
        {
            indexMap.put( crs, mock( SpatialKnownIndex.class ) );
        }

        fusionIndexAccessor = new SpatialFusionIndexAccessor( indexMap, 0, mock( IndexDescriptor.class ), null, indexFactory );
    }

    @Test
    public void dropMustDropAll() throws Exception
    {
        fusionIndexAccessor.drop();

        for ( SpatialKnownIndex spatialKnownIndex : indexMap.values() )
        {
            verify( spatialKnownIndex, times( 1 ) ).drop();
        }
    }

    @Test
    public void dropMustThrowIfDropOneFail() throws Exception
    {
        verifyFailOnSingleDropFailure( indexMap.get( WGS84 ) );
        verify( indexMap.get( Cartesian ), times( 1 ) ).drop();
        reset( indexMap.values().toArray() );
        verifyFailOnSingleDropFailure( indexMap.get( Cartesian ) );
        verify( indexMap.get( WGS84 ), times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfBothFail() throws Exception
    {
        // given
        IOException wgsFailure = new IOException( "wgs" );
        IOException cartesianFailure = new IOException( "cartesian" );
        doThrow( wgsFailure ).when( indexMap.get( WGS84 ) ).drop();
        doThrow( cartesianFailure ).when( indexMap.get( Cartesian ) ).drop();

        try
        {
            // when
            fusionIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( e, anyOf( sameInstance( wgsFailure ), sameInstance( cartesianFailure ) ) );
        }
    }

    @Test
    public void clostMustCloseAll() throws Exception
    {
        fusionIndexAccessor.close();

        for ( SpatialKnownIndex spatialKnownIndex : indexMap.values() )
        {
            verify( spatialKnownIndex, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustCloseOthersIfCloseOneFail() throws Exception
    {
        for ( SpatialKnownIndex failingIndex : indexMap.values() )
        {
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( failingIndex ).close();

            // when
            try
            {
                fusionIndexAccessor.close();
                fail( "Should have failed" );
            }
            catch ( IOException ignore )
            {
            }

            // then
            for ( SpatialKnownIndex successfulIndex : indexMap.values() )
            {
                if ( failingIndex != successfulIndex )
                {
                    verify( successfulIndex, Mockito.times( 1 ) ).close();
                }
            }
            reset( indexMap.values().toArray() );
        }
    }

    @Test
    public void closeMustThrowIfCloseOneFail() throws Exception
    {
        for ( SpatialKnownIndex index : indexMap.values() )
        {
            IOException expectedFailure = new IOException( "fail" );
            doThrow( expectedFailure ).when( index ).close();
            try
            {
                fusionIndexAccessor.close();
                fail( "Should have failed" );
            }
            catch ( IOException e )
            {
                assertSame( expectedFailure, e );
            }
            reset( indexMap.values().toArray() );
        }
    }

    @Test
    public void closeMustThrowIfAllFail() throws Exception
    {
        // given
        List<IOException> failures = new ArrayList<>();
        for ( SpatialKnownIndex index : indexMap.values() )
        {
            IOException exception = new IOException( "unknown" );
            failures.add( exception );
            doThrow( exception ).when( index ).close();
        }

        try
        {
            // when
            fusionIndexAccessor.close();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            List<Matcher<? super IOException>> matchers = new ArrayList<>();
            for ( IOException failure : failures )
            {
                matchers.add( sameInstance( failure ) );
            }
            assertThat( e, anyOf( matchers ) );
        }
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAll() throws Exception
    {
        // given
        long[] wgsEntries = {0, 1, 2, 5, 6};
        long[] cartesianEntries = {3, 4, 7, 8};
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), cartesianEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, wgsEntries );
        assertResultContainsAll( result, cartesianEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithWGSEmpty() throws Exception
    {
        // given
        long[] wgsEntries = new long[0];
        long[] cartesianEntries = {3, 4, 7, 8};
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), cartesianEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, wgsEntries );
        assertResultContainsAll( result, cartesianEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithCartesianEmpty() throws Exception
    {
        // given
        long[] wgsEntries = {0, 1, 2, 5, 6};
        long[] cartesianEntries = new long[0];
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), cartesianEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, wgsEntries );
        assertResultContainsAll( result, cartesianEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithAllEmpty() throws Exception
    {
        // given
        long[] wgsEntries = new long[0];
        long[] cartesianEntries = new long[0];
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), cartesianEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, wgsEntries );
        assertResultContainsAll( result, cartesianEntries );
    }

    @Test
    public void allEntriesReaderMustCloseAll() throws Exception
    {
        // given
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( Cartesian ), new long[0] );

        // when
        fusionIndexAccessor.newAllEntriesReader().close();

        // then
        verify( wgsAllEntriesReader, VerificationModeFactory.times( 1 ) ).close();
        verify( cartesianAllEntriesReader, VerificationModeFactory.times( 1 ) ).close();
    }

    @Test
    public void allEntriesReaderMustCloseOtherIfOneThrow() throws Exception
    {
        // given
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( Cartesian ), new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( wgsAllEntriesReader, fusionAllEntriesReader, cartesianAllEntriesReader );
        reset( wgsAllEntriesReader, cartesianAllEntriesReader );
        fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( cartesianAllEntriesReader, fusionAllEntriesReader, wgsAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustThrowIfOneThrow() throws Exception
    {
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( indexMap.get( Cartesian ), new long[0] );

        // then
        verifyFusionCloseThrowOnSingleCloseThrow( wgsAllEntriesReader, fusionIndexAccessor.newAllEntriesReader() );
        reset( wgsAllEntriesReader, cartesianAllEntriesReader );
        verifyFusionCloseThrowOnSingleCloseThrow( cartesianAllEntriesReader, fusionIndexAccessor.newAllEntriesReader() );
    }

    @Test
    public void allEntriesReaderMustReportFusionUnknownMaxCountIfWGSReportUnknownMaxCount() throws Exception
    {
        mockSingleAllEntriesReaderWithUnknownMaxCount( indexMap.get( WGS84 ), new long[0] );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportFusionUnknownMaxCountIfCartesianReportUnknownMaxCount() throws Exception
    {
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), new long[0] );
        mockSingleAllEntriesReaderWithUnknownMaxCount( indexMap.get( Cartesian ), new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportFusionMacCountOfAll() throws Exception
    {
        mockSingleAllEntriesReader( indexMap.get( WGS84 ), new long[]{0, 1, 2, 5, 6} );
        mockSingleAllEntriesReader( indexMap.get( Cartesian ), new long[]{3, 4, 7, 8} );

        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( 9L ) );
    }

    private void verifyFailOnSingleDropFailure( SpatialKnownIndex spatialKnownIndex ) throws IOException
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( spatialKnownIndex ).drop();
        try
        {
            fusionIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertSame( expectedFailure, e );
        }
    }

    private BoundedIterable<Long> mockSingleAllEntriesReader( SpatialKnownIndex spatialKnownIndex, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReader( entries );
        when( spatialKnownIndex.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    private void mockSingleAllEntriesReaderWithUnknownMaxCount( SpatialKnownIndex spatialKnownIndex, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReaderUnknownMaxCount( entries );
        when( spatialKnownIndex.newAllEntriesReader() ).thenReturn( allEntriesReader );
    }
}
