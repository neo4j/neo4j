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
package org.neo4j.kernel.impl.index.schema.combined;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.index.IndexAccessor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyCombinedCloseThrowIfBothThrow;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyCombinedCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyOtherIsClosedOnSingleThrow;

public class CombinedIndexAccessorTest
{
    private IndexAccessor boostAccessor;
    private IndexAccessor fallbackAccessor;
    private CombinedIndexAccessor combinedIndexAccessor;

    @Before
    public void setup()
    {
        boostAccessor = mock( IndexAccessor.class );
        fallbackAccessor = mock( IndexAccessor.class );
        combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );
    }

    /* drop */

    @Test
    public void dropMustDropBoostAndFallback() throws Exception
    {
        // when
        // ... both drop successful
        combinedIndexAccessor.drop();
        // then
        verify( boostAccessor, times( 1 ) ).drop();
        verify( fallbackAccessor, times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfDropBoostFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropFallbackFail() throws Exception
    {
        // when
        verifyFailOnSingleDropFailure( fallbackAccessor, combinedIndexAccessor );
    }

    private void verifyFailOnSingleDropFailure( IndexAccessor failingAccessor, CombinedIndexAccessor combinedIndexAccessor )
            throws IOException
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( failingAccessor ).drop();
        try
        {
            combinedIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertSame( expectedFailure, e );
        }
    }

    @Test
    public void dropMustThrowIfBothFail() throws Exception
    {
        // given
        IOException boostFailure = new IOException( "boost" );
        IOException fallbackFailure = new IOException( "fallback" );
        doThrow( boostFailure ).when( boostAccessor ).drop();
        doThrow( fallbackFailure ).when( fallbackAccessor ).drop();

        try
        {
            // when
            combinedIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( e, anyOf( sameInstance( boostFailure ), sameInstance( fallbackFailure ) ) );
        }
    }

    /* close */

    @Test
    public void closeMustCloseBoostAndFallback() throws Exception
    {
        // when
        // ... both drop successful
        combinedIndexAccessor.close();

        // then
        verify( boostAccessor, times( 1 ) ).close();
        verify( fallbackAccessor, times( 1 ) ).close();
    }

    @Test
    public void closeMustThrowIfFallbackThrow() throws Exception
    {
        verifyCombinedCloseThrowOnSingleCloseThrow( fallbackAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustThrowIfBoostThrow() throws Exception
    {
        verifyCombinedCloseThrowOnSingleCloseThrow( boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustCloseBoostIfFallbackThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( fallbackAccessor, boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustCloseFallbackIfBoostThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( boostAccessor, fallbackAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustThrowIfBothFail() throws Exception
    {
        verifyCombinedCloseThrowIfBothThrow( boostAccessor, fallbackAccessor, combinedIndexAccessor );
    }

    // newAllEntriesReader

    @Test
    public void allEntriesReaderMustCombineResultFromBoostAndFallback() throws Exception
    {
        // given
        long[] boostEntries = {0, 1, 2, 5, 6};
        long[] fallbackEntries = {3, 4, 7, 8};
        mockAllEntriesReaders( boostEntries, fallbackEntries );

        // when
        Set<Long> result = Iterables.asSet( combinedIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, boostEntries );
        assertResultContainsAll( result, fallbackEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromBoostAndFallbackWithEmptyBoost() throws Exception
    {
        // given
        long[] boostEntries = new long[0];
        long[] fallbackEntries = {3, 4, 7, 8};
        mockAllEntriesReaders( boostEntries, fallbackEntries );

        // when
        Set<Long> result = Iterables.asSet( combinedIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, boostEntries );
        assertResultContainsAll( result, fallbackEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromBoostAndFallbackWithEmptyFallback() throws Exception
    {
        // given
        long[] boostEntries = {0, 1, 2, 5, 6};
        long[] fallbackEntries = new long[0];
        mockAllEntriesReaders( boostEntries, fallbackEntries );

        // when
        Set<Long> result = Iterables.asSet( combinedIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, boostEntries );
        assertResultContainsAll( result, fallbackEntries );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromBoostAndFallbackBothEmpty() throws Exception
    {
        // given
        long[] boostEntries = new long[0];
        long[] fallbackEntries = new long[0];
        mockAllEntriesReaders( boostEntries, fallbackEntries );

        // when
        Set<Long> result = Iterables.asSet( combinedIndexAccessor.newAllEntriesReader() );

        // then
        assertResultContainsAll( result, boostEntries );
        assertResultContainsAll( result, fallbackEntries );
        assertTrue( result.isEmpty() );
    }

    @Test
    public void allEntriesReaderMustCloseBothBoostAndFallback() throws Exception
    {
        // given
        BoundedIterable<Long> boostAllEntriesReader = mockSingleAllEntriesReader( boostAccessor, new long[0] );
        BoundedIterable<Long> fallbackAllEntriesReader = mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // when
        combinedIndexAccessor.newAllEntriesReader().close();

        // then
        verify( boostAllEntriesReader, times( 1 ) ).close();
        verify( fallbackAllEntriesReader, times( 1 ) ).close();
    }

    @Test
    public void allEntriesReaderMustCloseBoostIfFallbackThrow() throws Exception
    {
        // given
        BoundedIterable<Long> boostAllEntriesReader = mockSingleAllEntriesReader( boostAccessor, new long[0] );
        BoundedIterable<Long> fallbackAllEntriesReader = mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( fallbackAllEntriesReader, boostAllEntriesReader, combinedAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustCloseFallbackIfBoostThrow() throws Exception
    {
        // given
        BoundedIterable<Long> boostAllEntriesReader = mockSingleAllEntriesReader( boostAccessor, new long[0] );
        BoundedIterable<Long> fallbackAllEntriesReader = mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        verifyOtherIsClosedOnSingleThrow( boostAllEntriesReader, fallbackAllEntriesReader, combinedAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustThrowIfFallbackThrow() throws Exception
    {
        // given
        mockSingleAllEntriesReader( boostAccessor, new long[0] );
        BoundedIterable<Long> fallbackAllEntriesReader = mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        CombinedIndexTestHelp.verifyCombinedCloseThrowOnSingleCloseThrow( fallbackAllEntriesReader, combinedAllEntriesReader );
    }

    @Test
    public void allEntriesReaderMustThrowIfBoostThrow() throws Exception
    {
        // given
        BoundedIterable<Long> boostAllEntriesReader = mockSingleAllEntriesReader( boostAccessor, new long[0] );
        mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        CombinedIndexTestHelp.verifyCombinedCloseThrowOnSingleCloseThrow( boostAllEntriesReader, combinedAllEntriesReader );

    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfBoostReportUnknownMaxCount() throws Exception
    {
        // given
        mockSingleAllEntriesReaderWithUnknownMaxCount( boostAccessor, new long[0] );
        mockSingleAllEntriesReader( fallbackAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        assertThat( combinedAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfFallbackReportUnknownMaxCount() throws Exception
    {
        // given
        mockSingleAllEntriesReaderWithUnknownMaxCount( fallbackAccessor, new long[0] );
        mockSingleAllEntriesReader( boostAccessor, new long[0] );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        assertThat( combinedAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
    }

    @Test
    public void allEntriesReaderMustReportCombinedMaxCountOfBoostAndFallback() throws Exception
    {
        mockSingleAllEntriesReader( boostAccessor, new long[]{1, 2} );
        mockSingleAllEntriesReader( fallbackAccessor, new long[]{3, 4} );

        // then
        BoundedIterable<Long> combinedAllEntriesReader = combinedIndexAccessor.newAllEntriesReader();
        assertThat( combinedAllEntriesReader.maxCount(), is( 4L ) );
    }

    private void assertResultContainsAll( Set<Long> result, long[] boostEntries )
    {
        for ( long boostEntry : boostEntries )
        {
            assertTrue( "Expected to contain " + boostEntry + ", but was " + result, result.contains( boostEntry ) );
        }
    }

    private void mockAllEntriesReaders( long[] boostEntries, long[] fallbackEntries )
    {
        mockSingleAllEntriesReader( boostAccessor, boostEntries );
        mockSingleAllEntriesReader( fallbackAccessor, fallbackEntries );
    }

    private BoundedIterable<Long> mockSingleAllEntriesReader( IndexAccessor targetAccessor, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReader( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    private BoundedIterable<Long> mockedAllEntriesReader( long... entries )
    {
        return mockedAllEntriesReader( true, entries );
    }

    private BoundedIterable<Long> mockSingleAllEntriesReaderWithUnknownMaxCount( IndexAccessor targetAccessor, long[] entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReaderUnknownMaxCount( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    private BoundedIterable<Long> mockedAllEntriesReaderUnknownMaxCount( long... entries )
    {
        return mockedAllEntriesReader( false, entries );
    }

    private BoundedIterable<Long> mockedAllEntriesReader( boolean knownMaxCount, long... entries )
    {
        BoundedIterable<Long> mockedAllEntriesReader = mock( BoundedIterable.class );
        when( mockedAllEntriesReader.maxCount() ).thenReturn( knownMaxCount ? entries.length : BoundedIterable.UNKNOWN_MAX_COUNT );
        when( mockedAllEntriesReader.iterator() ).thenReturn( Iterators.asIterator(entries ) );
        return mockedAllEntriesReader;
    }
}
