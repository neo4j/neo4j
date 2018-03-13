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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;

public class FusionIndexAccessorTest
{
    private IndexAccessor nativeAccessor;
    private IndexAccessor spatialAccessor;
    private IndexAccessor temporalAccessor;
    private IndexAccessor luceneAccessor;
    private FusionIndexAccessor fusionIndexAccessor;
    private final long indexId = 10;
    private final DropAction dropAction = mock( DropAction.class );
    private List<IndexAccessor> allAccessors;

    @Before
    public void setup()
    {
        nativeAccessor = mock( IndexAccessor.class );
        spatialAccessor = mock( IndexAccessor.class );
        temporalAccessor = mock( IndexAccessor.class );
        luceneAccessor = mock( IndexAccessor.class );
        allAccessors = Arrays.asList( nativeAccessor, spatialAccessor, temporalAccessor, luceneAccessor);
        fusionIndexAccessor = new FusionIndexAccessor( nativeAccessor, spatialAccessor, temporalAccessor,
                luceneAccessor, new FusionSelector(), indexId, mock( SchemaIndexDescriptor.class ), dropAction );
    }

    /* drop */

    @Test
    public void dropMustDropAll() throws Exception
    {
        // when
        // ... all drop successful
        fusionIndexAccessor.drop();
        // then
        for ( IndexAccessor accessor : allAccessors )
        {
            verify( accessor, times( 1 ) ).drop();
        }
        verify( dropAction ).drop( indexId );
    }

    @Test
    public void dropMustThrowIfDropNativeFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( nativeAccessor, fusionIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropSpatialFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( spatialAccessor, fusionIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropTemporalFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( temporalAccessor, fusionIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropLuceneFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( luceneAccessor, fusionIndexAccessor );
    }

    @Test
    public void fusionIndexIsDirtyWhenNativeIndexIsDirty()
    {
        when( nativeAccessor.isDirty() ).thenReturn( true ).thenReturn( false );

        assertTrue( fusionIndexAccessor.isDirty() );
        assertFalse( fusionIndexAccessor.isDirty() );
    }

    @Test
    public void fusionIndexIsDirtyWhenSpatialIndexIsDirty()
    {
        when( spatialAccessor.isDirty() ).thenReturn( true ).thenReturn( false );

        assertTrue( fusionIndexAccessor.isDirty() );
        assertFalse( fusionIndexAccessor.isDirty() );
    }

    @Test
    public void fusionIndexIsDirtyWhenTemporalIndexIsDirty()
    {
        when( temporalAccessor.isDirty() ).thenReturn( true ).thenReturn( false );

        assertTrue( fusionIndexAccessor.isDirty() );
        assertFalse( fusionIndexAccessor.isDirty() );
    }

    private void verifyFailOnSingleDropFailure( IndexAccessor failingAccessor, FusionIndexAccessor fusionIndexAccessor )
            throws IOException
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

    @Test
    public void dropMustThrowIfAllFail() throws Exception
    {
        // given
        IOException nativeFailure = new IOException( "native" );
        IOException spatialFailure = new IOException( "spatial" );
        IOException temporalFailure = new IOException( "temporal" );
        IOException luceneFailure = new IOException( "lucene" );
        doThrow( nativeFailure ).when( nativeAccessor ).drop();
        doThrow( spatialFailure ).when( spatialAccessor ).drop();
        doThrow( temporalFailure ).when( temporalAccessor ).drop();
        doThrow( luceneFailure ).when( luceneAccessor ).drop();

        try
        {
            // when
            fusionIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( e, anyOf(
                    sameInstance( nativeFailure ),
                    sameInstance( spatialFailure ),
                    sameInstance( temporalFailure ),
                    sameInstance( luceneFailure ) ) );
        }
    }

    /* close */

    @Test
    public void closeMustCloseAll() throws Exception
    {
        // when
        // ... all close successful
        fusionIndexAccessor.close();

        // then
        for ( IndexAccessor accessor : allAccessors )
        {
            verify( accessor, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustThrowIfLuceneThrow() throws Exception
    {
        verifyFusionCloseThrowOnSingleCloseThrow( luceneAccessor, fusionIndexAccessor );
    }

    @Test
    public void closeMustThrowIfSpatialThrow() throws Exception
    {
        verifyFusionCloseThrowOnSingleCloseThrow( spatialAccessor, fusionIndexAccessor );
    }

    @Test
    public void closeMustThrowIfTemporalThrow() throws Exception
    {
        verifyFusionCloseThrowOnSingleCloseThrow( temporalAccessor, fusionIndexAccessor );
    }

    @Test
    public void closeMustThrowIfNativeThrow() throws Exception
    {
        verifyFusionCloseThrowOnSingleCloseThrow( nativeAccessor, fusionIndexAccessor );
    }

    @Test
    public void closeMustCloseOthersIfLuceneThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( luceneAccessor, fusionIndexAccessor, nativeAccessor, spatialAccessor, temporalAccessor );
    }

    @Test
    public void closeMustCloseOthersIfSpatialThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( spatialAccessor, fusionIndexAccessor, nativeAccessor, temporalAccessor, luceneAccessor );
    }

    @Test
    public void closeMustCloseOthersIfTemporalThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( temporalAccessor, fusionIndexAccessor, nativeAccessor, spatialAccessor, luceneAccessor );
    }

    @Test
    public void closeMustCloseOthersIfNativeThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( nativeAccessor, fusionIndexAccessor, luceneAccessor, spatialAccessor, temporalAccessor );
    }

    @Test
    public void closeMustThrowIfAllFail() throws Exception
    {
        verifyFusionCloseThrowIfAllThrow( fusionIndexAccessor, nativeAccessor, spatialAccessor, temporalAccessor, luceneAccessor );
    }

    // newAllEntriesReader

    @Test
    public void allEntriesReaderMustCombineResultFromAll()
    {
        // given
        long[] nativeEntries = {0, 1, 6, 13, 14};
        long[] spatialEntries = {2, 5, 9};
        long[] temporalEntries = {4, 8, 11};
        long[] luceneEntries = {3, 7, 10, 12};
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithEmptyNative()
    {
        // given
        long[] nativeEntries = new long[0];
        long[] spatialEntries = {2, 5, 9};
        long[] temporalEntries = {4, 8, 11};
        long[] luceneEntries = {3, 4, 7, 8};
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
    }
    @Test
    public void allEntriesReaderMustCombineResultFromAllWithEmptySpatial()
    {
        // given
        long[] nativeEntries = {0, 1, 6, 13, 14};
        long[] spatialEntries = new long[0];
        long[] temporalEntries = {4, 8, 11};
        long[] luceneEntries = {3, 7, 10, 12};
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithEmptyTemporal()
    {
        // given
        long[] nativeEntries = {0, 1, 6, 13, 14};
        long[] spatialEntries = {2, 5, 9};
        long[] temporalEntries = new long[0];
        long[] luceneEntries = {3, 7, 10, 12};
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithEmptyLucene()
    {
        // given
        long[] nativeEntries = {0, 1, 2, 5, 6};
        long[] spatialEntries = {2, 5, 9};
        long[] temporalEntries = {4, 8, 11};
        long[] luceneEntries = new long[0];
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllEmpty()
    {
        // given
        long[] nativeEntries = new long[0];
        long[] spatialEntries = new long[0];
        long[] temporalEntries = new long[0];
        long[] luceneEntries = new long[0];
        mockAllEntriesReaders( nativeEntries, spatialEntries, temporalEntries, luceneEntries );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, nativeEntries );
        assertResultContainsAll( result, spatialEntries );
        assertResultContainsAll( result, temporalEntries );
        assertResultContainsAll( result, luceneEntries );
        assertTrue( result.isEmpty() );
    }

    @Test
    public void allEntriesReaderMustCloseAll() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // when
        fusionIndexAccessor.newAllEntriesReader().close();

        // then
        verify( nativeAllEntriesReader, times( 1 ) ).close();
        verify( spatialAllEntriesReader, times( 1 ) ).close();
        verify( temporalAllEntriesReader, times( 1 ) ).close();
        verify( luceneAllEntriesReader, times( 1 ) ).close();
    }

    @Test
    public void allEntriesReaderMustCloseNativeIfLuceneThrow() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( luceneAllEntriesReader, fusionAllEntriesReader, nativeAllEntriesReader, spatialAllEntriesReader,
                temporalAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustCloseAllIfSpatialThrow() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( spatialAllEntriesReader, fusionAllEntriesReader, luceneAllEntriesReader, nativeAllEntriesReader,
                temporalAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustCloseAllIfTemporalThrow() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( temporalAllEntriesReader, fusionAllEntriesReader, luceneAllEntriesReader, spatialAllEntriesReader,
                nativeAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustCloseAllIfNativeThrow() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( nativeAllEntriesReader, fusionAllEntriesReader, luceneAllEntriesReader, spatialAllEntriesReader,
                temporalAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustThrowIfLuceneThrow() throws Exception
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        BoundedIterable<Long> luceneAllEntriesReader = mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( luceneAllEntriesReader, fusionAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustThrowIfNativeThrow() throws Exception
    {
        // given
        BoundedIterable<Long> nativeAllEntriesReader = mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( nativeAllEntriesReader, fusionAllEntriesReader );

    }

    @Test
    public void allEntriesReaderMustThrowIfSpatialThrow() throws Exception
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        BoundedIterable<Long> spatialAllEntriesReader = mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( spatialAllEntriesReader, fusionAllEntriesReader );

    }

    @Test
    public void allEntriesReaderMustThrowIfTemporalThrow() throws Exception
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        BoundedIterable<Long> temporalAllEntriesReader = mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( temporalAllEntriesReader, fusionAllEntriesReader );

    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfNativeReportUnknownMaxCount()
    {
        // given
        mockSingleAllEntriesReaderWithUnknownMaxCount( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfSpatialReportUnknownMaxCount() throws Exception
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReaderWithUnknownMaxCount( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfTemporalReportUnknownMaxCount() throws Exception
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReaderWithUnknownMaxCount( temporalAccessor, new long[0] );
        mockSingleAllEntriesReader( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfLuceneReportUnknownMaxCount()
    {
        // given
        mockSingleAllEntriesReader( nativeAccessor, new long[0] );
        mockSingleAllEntriesReader( spatialAccessor, new long[0] );
        mockSingleAllEntriesReader( temporalAccessor, new long[0] );
        mockSingleAllEntriesReaderWithUnknownMaxCount( luceneAccessor, new long[0] );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportFusionMaxCountOfNativeAndLucene()
    {
        mockSingleAllEntriesReader( nativeAccessor, new long[]{1, 2} );
        mockSingleAllEntriesReader( spatialAccessor, new long[]{3, 4} );
        mockSingleAllEntriesReader( temporalAccessor, new long[]{5, 6} );
        mockSingleAllEntriesReader( luceneAccessor, new long[]{7, 8} );

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( 8L ) );
    }

    static void assertResultContainsAll( Set<Long> result, long[] nativeEntries )
    {
        for ( long nativeEntry : nativeEntries )
        {
            assertTrue( "Expected to contain " + nativeEntry + ", but was " + result, result.contains( nativeEntry ) );
        }
    }

    private static BoundedIterable<Long> mockSingleAllEntriesReader( IndexAccessor targetAccessor, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReader( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    static BoundedIterable<Long> mockedAllEntriesReader( long... entries )
    {
        return mockedAllEntriesReader( true, entries );
    }

    private static void mockSingleAllEntriesReaderWithUnknownMaxCount( IndexAccessor targetAccessor, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReaderUnknownMaxCount( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
    }

    static BoundedIterable<Long> mockedAllEntriesReaderUnknownMaxCount( long... entries )
    {
        return mockedAllEntriesReader( false, entries );
    }

    static BoundedIterable<Long> mockedAllEntriesReader( boolean knownMaxCount, long... entries )
    {
        BoundedIterable<Long> mockedAllEntriesReader = mock( BoundedIterable.class );
        when( mockedAllEntriesReader.maxCount() ).thenReturn( knownMaxCount ? entries.length : BoundedIterable.UNKNOWN_MAX_COUNT );
        when( mockedAllEntriesReader.iterator() ).thenReturn( Iterators.asIterator(entries ) );
        return mockedAllEntriesReader;
    }

    private void mockAllEntriesReaders( long[] numberEntries, long[] spatialEntries, long[] temporalEntries, long[] luceneEntries )
    {
        mockSingleAllEntriesReader( nativeAccessor, numberEntries );
        mockSingleAllEntriesReader( spatialAccessor, spatialEntries );
        mockSingleAllEntriesReader( temporalAccessor, temporalEntries );
        mockSingleAllEntriesReader( luceneAccessor, luceneEntries );
    }
}
