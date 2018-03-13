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
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.helpers.ArrayUtil.without;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;

public class FusionIndexAccessorTest
{
    private IndexAccessor stringAccessor;
    private IndexAccessor numberAccessor;
    private IndexAccessor spatialAccessor;
    private IndexAccessor temporalAccessor;
    private IndexAccessor luceneAccessor;
    private FusionIndexAccessor fusionIndexAccessor;
    private final long indexId = 10;
    private final DropAction dropAction = mock( DropAction.class );
    private IndexAccessor[] allAccessors;

    @Rule
    public RandomRule random = new RandomRule();

    @Before
    public void mockComponents()
    {
        stringAccessor = mock( IndexAccessor.class );
        numberAccessor = mock( IndexAccessor.class );
        spatialAccessor = mock( IndexAccessor.class );
        temporalAccessor = mock( IndexAccessor.class );
        luceneAccessor = mock( IndexAccessor.class );
        allAccessors = array( stringAccessor, numberAccessor, spatialAccessor, temporalAccessor, luceneAccessor );
        fusionIndexAccessor = new FusionIndexAccessor( allAccessors, new FusionSelector(), indexId, mock( SchemaIndexDescriptor.class ), dropAction );
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
    public void dropMustThrowIfDropAnyFail() throws Exception
    {
        for ( IndexAccessor accessor : allAccessors )
        {
            // when
            verifyFailOnSingleDropFailure( accessor, fusionIndexAccessor );
        }
    }

    @Test
    public void fusionIndexIsDirtyWhenAnyIsDirty()
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            // when
            for ( int j = 0; j < allAccessors.length; j++ )
            {
                when( allAccessors[j].isDirty() ).thenReturn( j == i );
            }

            // then
            assertTrue( fusionIndexAccessor.isDirty() );
        }
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
        doAnswer( invocation -> null ).when( failingAccessor ).drop();
    }

    @Test
    public void dropMustThrowIfAllFail() throws Exception
    {
        // given
        List<IOException> exceptions = new ArrayList<>();
        for ( IndexAccessor indexAccessor : allAccessors )
        {
            IOException exception = new IOException( indexAccessor.getClass().getSimpleName() + " fail" );
            exceptions.add( exception );
            doThrow( exception ).when( indexAccessor ).drop();
        }

        try
        {
            // when
            fusionIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( exceptions, hasItem( e ) );
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
    public void closeMustThrowIfOneThrow() throws Exception
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            verifyFusionCloseThrowOnSingleCloseThrow( allAccessors[i], fusionIndexAccessor );
            mockComponents();
        }
    }

    @Test
    public void closeMustCloseOthersIfOneThrow() throws Exception
    {
        int count = allAccessors.length;
        for ( int i = 0; i < count; i++ )
        {
            IndexAccessor accessor = allAccessors[i];
            verifyOtherIsClosedOnSingleThrow( accessor, fusionIndexAccessor, without( allAccessors, accessor ) );
            mockComponents();
        }
    }

    @Test
    public void closeMustThrowIfAllFail() throws Exception
    {
        verifyFusionCloseThrowIfAllThrow( fusionIndexAccessor, allAccessors );
    }

    // newAllEntriesReader

    @Test
    public void allEntriesReaderMustCombineResultFromAll()
    {
        // given
        List<Long>[] ids = new List[allAccessors.length];
        long lastId = 0;
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = Arrays.asList( lastId++, lastId++ );
        }
        mockAllEntriesReaders( ids );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        for ( List<Long> part : ids )
        {
            assertResultContainsAll( result, part );
        }
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllWithOneEmpty()
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            // given
            List<Long>[] ids = new List[allAccessors.length];
            long lastId = 0;
            for ( int j = 0; j < ids.length; j++ )
            {
                ids[j] = j == i ? Arrays.asList() : Arrays.asList( lastId++, lastId++ );
            }
            mockAllEntriesReaders( ids );

            // when
            Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

            // then
            for ( List<Long> part : ids )
            {
                assertResultContainsAll( result, part );
            }
        }
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllEmpty()
    {
        // given
        List<Long>[] ids = new List[allAccessors.length];
        for ( int j = 0; j < ids.length; j++ )
        {
            ids[j] = Arrays.asList();
        }
        mockAllEntriesReaders( ids );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertTrue( result.isEmpty() );
    }

    @Test
    public void allEntriesReaderMustCombineResultFromAllAccessors()
    {
        // given
        List<Long>[] parts = new List[allAccessors.length];
        for ( int i = 0; i < parts.length; i++ )
        {
            parts[i] = new ArrayList<>();
        }
        for ( long i = 0; i < 10; i++ )
        {
            random.among( parts ).add( i );
        }
        mockAllEntriesReaders( parts );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        for ( List<Long> part : parts )
        {
            assertResultContainsAll( result, part );
        }
    }

    @Test
    public void allEntriesReaderMustCloseAll() throws Exception
    {
        // given
        BoundedIterable<Long>[] allEntriesReaders = Arrays.stream( allAccessors ).map( accessor -> mockSingleAllEntriesReader( accessor, Arrays.asList() ) )
                .toArray( BoundedIterable[]::new );

        // when
        fusionIndexAccessor.newAllEntriesReader().close();

        // then
        for ( BoundedIterable<Long> allEntriesReader : allEntriesReaders )
        {
            verify( allEntriesReader, times( 1 ) ).close();
        }
    }

    @Test
    public void allEntriesReaderMustCloseOthersIfOneThrow() throws Exception
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            // given
            BoundedIterable<Long>[] allEntriesReaders =
                    Arrays.stream( allAccessors ).map( accessor -> mockSingleAllEntriesReader( accessor, Arrays.asList() ) ).toArray( BoundedIterable[]::new );

            // then
            BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
            verifyOtherIsClosedOnSingleThrow( allEntriesReaders[i], fusionAllEntriesReader, without( allEntriesReaders, allEntriesReaders[i] ) );

            mockComponents();
        }
    }

    @Test
    public void allEntriesReaderMustThrowIfOneThrow() throws Exception
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            // given
            BoundedIterable<Long> allEntriesReader = null;
            for ( int j = 0; j < allAccessors.length; j++ )
            {
                BoundedIterable<Long> reader = mockSingleAllEntriesReader( allAccessors[j], Arrays.asList() );
                if ( j == i )
                {
                    allEntriesReader = reader;
                }
            }

            // then
            BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
            FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( allEntriesReader, fusionAllEntriesReader );
        }
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfAnyReportUnknownMaxCount()
    {
        for ( int i = 0; i < allAccessors.length; i++ )
        {
            for ( int j = 0; j < allAccessors.length; j++ )
            {
                // given
                if ( j == i )
                {
                    mockSingleAllEntriesReaderWithUnknownMaxCount( allAccessors[j], Arrays.asList() );
                }
                else
                {
                    mockSingleAllEntriesReader( allAccessors[j], Arrays.asList() );
                }
            }

            // then
            BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
            assertThat( fusionAllEntriesReader.maxCount(), is( BoundedIterable.UNKNOWN_MAX_COUNT ) );
        }
    }

    @Test
    public void allEntriesReaderMustReportFusionMaxCountOfAll()
    {
        long lastId = 0;
        for ( IndexAccessor accessor : allAccessors )
        {
            mockSingleAllEntriesReader( accessor, Arrays.asList( lastId++, lastId++ ) );
        }

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( lastId ) );
    }

    static void assertResultContainsAll( Set<Long> result, List<Long> expectedEntries )
    {
        for ( long expectedEntry : expectedEntries )
        {
            assertTrue( "Expected to contain " + expectedEntry + ", but was " + result, result.contains( expectedEntry ) );
        }
    }

    private static BoundedIterable<Long> mockSingleAllEntriesReader( IndexAccessor targetAccessor, List<Long> entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReader( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    static BoundedIterable<Long> mockedAllEntriesReader( List<Long> entries )
    {
        return mockedAllEntriesReader( true, entries );
    }

    private static BoundedIterable<Long> mockSingleAllEntriesReaderWithUnknownMaxCount( IndexAccessor targetAccessor, List<Long> entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReaderUnknownMaxCount( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    static BoundedIterable<Long> mockedAllEntriesReaderUnknownMaxCount( List<Long> entries )
    {
        return mockedAllEntriesReader( false, entries );
    }

    static BoundedIterable<Long> mockedAllEntriesReader( boolean knownMaxCount, List<Long> entries )
    {
        BoundedIterable<Long> mockedAllEntriesReader = mock( BoundedIterable.class );
        when( mockedAllEntriesReader.maxCount() ).thenReturn( knownMaxCount ? entries.size() : BoundedIterable.UNKNOWN_MAX_COUNT );
        when( mockedAllEntriesReader.iterator() ).thenReturn( entries.iterator() );
        return mockedAllEntriesReader;
    }

    private void mockAllEntriesReaders( List<Long>... entries )
    {
        for ( int i = 0; i < entries.length; i++ )
        {
            mockSingleAllEntriesReader( allAccessors[i], entries[i] );
        }
    }

    private List<IndexAccessor> allAccessors()
    {
        return Arrays.asList( numberAccessor, stringAccessor, spatialAccessor, luceneAccessor );
    }
}
