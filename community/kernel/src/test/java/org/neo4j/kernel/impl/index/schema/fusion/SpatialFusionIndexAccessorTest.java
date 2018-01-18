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

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.verification.VerificationModeFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexAccessor;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.assertResultContainsAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.mockSingleAllEntriesReader;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexAccessorTest.mockSingleAllEntriesReaderWithUnknownMaxCount;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class SpatialFusionIndexAccessorTest
{

    private SpatialFusionIndexAccessor fusionIndexAccessor;
    private Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap = new HashMap<>();
    private Map<CoordinateReferenceSystem,IndexAccessor> accessorMap = new HashMap<>();

    @Before
    public void setup() throws Exception
    {
        SpatialKnownIndex.Factory indexFactory = mock( SpatialKnownIndex.Factory.class );

        for ( CoordinateReferenceSystem crs : asList( WGS84, Cartesian ) )
        {
            indexMap.put( crs, mock( SpatialKnownIndex.class ) );
            accessorMap.put( crs, mock( IndexAccessor.class ) );

            when( indexMap.get( crs ).getOnlineAccessor( any(), any() ) ).thenReturn( accessorMap.get( crs ) );
        }

        fusionIndexAccessor = new SpatialFusionIndexAccessor( indexMap, 0, mock( IndexDescriptor.class ), null, indexFactory );
    }

    @Test
    public void dropMustDropAll() throws Exception
    {
        fusionIndexAccessor.drop();

        for ( IndexAccessor accessor : accessorMap.values() )
        {
            verify( accessor, times( 1 ) ).drop();
        }
    }

    @Test
    public void dropMustThrowIfDropOneFail() throws Exception
    {
        verifyFailOnSingleDropFailure( accessorMap.get( WGS84 ) );
        verify( accessorMap.get( Cartesian ), times( 1 ) ).drop();
        reset( accessorMap.values().toArray() );
        verifyFailOnSingleDropFailure( accessorMap.get( Cartesian ) );
        verify( accessorMap.get( WGS84 ), times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfBothFail() throws Exception
    {
        // given
        IOException wgsFailure = new IOException( "wgs" );
        IOException cartesianFailure = new IOException( "cartesian" );
        doThrow( wgsFailure ).when( accessorMap.get( WGS84 ) ).drop();
        doThrow( cartesianFailure ).when( accessorMap.get( Cartesian ) ).drop();

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

        for ( IndexAccessor accessor : accessorMap.values() )
        {
            verify( accessor, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustCloseOthersIfCloseOneFail() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( accessorMap.get( WGS84 ), fusionIndexAccessor, accessorMap.get( Cartesian ) );
        reset( accessorMap.values().toArray() );
        verifyOtherIsClosedOnSingleThrow( accessorMap.get( Cartesian ), fusionIndexAccessor, accessorMap.get( WGS84 ) );
    }

    @Test
    public void closeMustThrowIfCloseOneFail() throws Exception
    {
        verifyFusionCloseThrowOnSingleCloseThrow( accessorMap.get( WGS84 ), fusionIndexAccessor );
        reset( accessorMap.values().toArray() );
        verifyFusionCloseThrowOnSingleCloseThrow( accessorMap.get( Cartesian ), fusionIndexAccessor );
    }

    @Test
    public void closeMustThrowIfAllFail() throws Exception
    {
        verifyFusionCloseThrowIfAllThrow( fusionIndexAccessor, accessorMap.values().toArray( new AutoCloseable[2] ) );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAll() throws Exception
    {
        // given
        long[] wgsEntries = {0, 1, 2, 5, 6};
        long[] cartesianEntries = {3, 4, 7, 8};
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), cartesianEntries );

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
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), cartesianEntries );

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
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), cartesianEntries );

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
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), wgsEntries );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), cartesianEntries );

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
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( Cartesian ), new long[0] );

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
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( Cartesian ), new long[0] );

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
        BoundedIterable<Long> wgsAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( WGS84 ), new long[0] );
        BoundedIterable<Long> cartesianAllEntriesReader = mockSingleAllEntriesReader( accessorMap.get( Cartesian ), new long[0] );

        // then
        verifyFusionCloseThrowOnSingleCloseThrow( wgsAllEntriesReader, fusionIndexAccessor.newAllEntriesReader() );
        reset( wgsAllEntriesReader, cartesianAllEntriesReader );
        verifyFusionCloseThrowOnSingleCloseThrow( cartesianAllEntriesReader, fusionIndexAccessor.newAllEntriesReader() );
    }

    @Test
    public void allEntriesReaderMustReportFusionUnknownMaxCountIfWGSReportUnknownMaxCount() throws Exception
    {
        mockSingleAllEntriesReaderWithUnknownMaxCount( accessorMap.get( WGS84 ), new long[0] );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportFusionUnknownMaxCountIfCartesianReportUnknownMaxCount() throws Exception
    {
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), new long[0] );
        mockSingleAllEntriesReaderWithUnknownMaxCount( accessorMap.get( Cartesian ), new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportFusionMacCountOfAll() throws Exception
    {
        mockSingleAllEntriesReader( accessorMap.get( WGS84 ), new long[]{0, 1, 2, 5, 6} );
        mockSingleAllEntriesReader( accessorMap.get( Cartesian ), new long[]{3, 4, 7, 8} );

        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( 9L ) );
    }

    private void verifyFailOnSingleDropFailure( IndexAccessor failingAccessor ) throws IOException
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( failingAccessor ).drop();
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
}
