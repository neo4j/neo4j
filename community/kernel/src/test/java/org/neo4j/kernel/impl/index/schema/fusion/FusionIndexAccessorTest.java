/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.helpers.ArrayUtil.without;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v00;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v10;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v20;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.TEMPORAL;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class FusionIndexAccessorTest
{
    private FusionIndexAccessor fusionIndexAccessor;
    private final long indexId = 10;
    private final DropAction dropAction = mock( DropAction.class );
    private IndexAccessor[] accessors;
    private IndexAccessor[] aliveAccessors;

    @Rule
    public RandomRule random = new RandomRule();

    @Parameterized.Parameters( name = "{0}" )
    public static FusionVersion[] versions()
    {
        return new FusionVersion[]
                {
                        v00, v10, v20
                };
    }

    @Parameterized.Parameter
    public static FusionVersion fusionVersion;

    @Before
    public void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        int[] activeSlots = fusionVersion.aliveSlots();
        accessors = new IndexAccessor[INSTANCE_COUNT];
        Arrays.fill( accessors, IndexAccessor.EMPTY );
        aliveAccessors = new IndexAccessor[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexAccessor mock = mock( IndexAccessor.class );
            aliveAccessors[i] = mock;
            switch ( activeSlots[i] )
            {
            case STRING:
                accessors[STRING] = mock;
                break;
            case NUMBER:
                accessors[NUMBER] = mock;
                break;
            case SPATIAL:
                accessors[SPATIAL] = mock;
                break;
            case TEMPORAL:
                accessors[TEMPORAL] = mock;
                break;
            case LUCENE:
                accessors[LUCENE] = mock;
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexAccessor = new FusionIndexAccessor( fusionVersion.slotSelector(), new InstanceSelector<>( accessors ), indexId,
                mock( SchemaIndexDescriptor.class ), dropAction );
    }

    private void resetMocks()
    {
        for ( IndexAccessor accessor : aliveAccessors )
        {
            reset( accessor );
        }
    }

    /* drop */

    @Test
    public void dropMustDropAll() throws Exception
    {
        // when
        // ... all drop successful
        fusionIndexAccessor.drop();

        // then
        for ( IndexAccessor accessor : aliveAccessors )
        {
            verify( accessor, times( 1 ) ).drop();
        }
        verify( dropAction ).drop( indexId );
    }

    @Test
    public void dropMustThrowIfDropAnyFail() throws Exception
    {
        for ( IndexAccessor accessor : aliveAccessors )
        {
            // when
            verifyFailOnSingleDropFailure( accessor, fusionIndexAccessor );
        }
    }

    @Test
    public void fusionIndexIsDirtyWhenAnyIsDirty()
    {
        for ( IndexAccessor dirtyAccessor : aliveAccessors )
        {
            // when
            for ( IndexAccessor aliveAccessor : aliveAccessors )
            {
                when( aliveAccessor.isDirty() ).thenReturn( aliveAccessor == dirtyAccessor );
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
        for ( IndexAccessor indexAccessor : aliveAccessors )
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
        for ( IndexAccessor accessor : aliveAccessors )
        {
            verify( accessor, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustThrowIfOneThrow() throws Exception
    {
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            IndexAccessor accessor = aliveAccessors[i];
            verifyFusionCloseThrowOnSingleCloseThrow( accessor, fusionIndexAccessor );
            initiateMocks();
        }
    }

    @Test
    public void closeMustCloseOthersIfOneThrow() throws Exception
    {
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            IndexAccessor accessor = aliveAccessors[i];
            verifyOtherIsClosedOnSingleThrow( accessor, fusionIndexAccessor, without( aliveAccessors, accessor ) );
            initiateMocks();
        }
    }

    @Test
    public void closeMustThrowIfAllFail() throws Exception
    {
        verifyFusionCloseThrowIfAllThrow( fusionIndexAccessor, aliveAccessors );
    }

    // newAllEntriesReader

    @Test
    public void allEntriesReaderMustCombineResultFromAll()
    {
        // given
        List<Long>[] ids = new List[aliveAccessors.length];
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
        for ( int i = 0; i < accessors.length; i++ )
        {
            // given
            List<Long>[] ids = new List[aliveAccessors.length];
            long lastId = 0;
            for ( int j = 0; j < ids.length; j++ )
            {
                ids[j] = j == i ? Collections.emptyList() : Arrays.asList( lastId++, lastId++ );
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
        List<Long>[] ids = new List[aliveAccessors.length];
        for ( int j = 0; j < ids.length; j++ )
        {
            ids[j] = Collections.emptyList();
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
        List<Long>[] parts = new List[aliveAccessors.length];
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
        BoundedIterable<Long>[] allEntriesReaders = Arrays.stream( aliveAccessors )
                .map( accessor -> mockSingleAllEntriesReader( accessor, Arrays.asList() ) )
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
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            // given
            BoundedIterable<Long>[] allEntriesReaders =
                    Arrays.stream( aliveAccessors )
                            .map( accessor -> mockSingleAllEntriesReader( accessor, Arrays.asList() ) )
                            .toArray( BoundedIterable[]::new );

            // then
            BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
            verifyOtherIsClosedOnSingleThrow( allEntriesReaders[i], fusionAllEntriesReader, without( allEntriesReaders, allEntriesReaders[i] ) );

            resetMocks();
        }
    }

    @Test
    public void allEntriesReaderMustThrowIfOneThrow() throws Exception
    {
        for ( IndexAccessor failingAccessor : aliveAccessors )
        {
            BoundedIterable<Long> failingReader = null;
            for ( IndexAccessor aliveAccessor : aliveAccessors )
            {
                BoundedIterable<Long> reader = mockSingleAllEntriesReader( aliveAccessor, Collections.emptyList() );
                if ( aliveAccessor == failingAccessor )
                {
                    failingReader = reader;
                }
            }

            // then
            BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
            FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( failingReader, fusionAllEntriesReader );
        }
    }

    @Test
    public void allEntriesReaderMustReportUnknownMaxCountIfAnyReportUnknownMaxCount()
    {
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            for ( int j = 0; j < aliveAccessors.length; j++ )
            {
                // given
                if ( j == i )
                {
                    mockSingleAllEntriesReaderWithUnknownMaxCount( aliveAccessors[j], Collections.emptyList() );
                }
                else
                {
                    mockSingleAllEntriesReader( aliveAccessors[j], Collections.emptyList() );
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
        for ( IndexAccessor accessor : aliveAccessors )
        {
            mockSingleAllEntriesReader( accessor, Arrays.asList( lastId++, lastId++ ) );
        }

        // then
        BoundedIterable<Long> fusionAllEntriesReader = fusionIndexAccessor.newAllEntriesReader();
        assertThat( fusionAllEntriesReader.maxCount(), is( lastId ) );
    }

    @Test
    public void shouldFailValueValidationIfAnyPartFail()
    {
        // given
        IllegalArgumentException failure = new IllegalArgumentException( "failing" );
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            for ( int j = 0; j < aliveAccessors.length; j++ )
            {
                if ( i == j )
                {
                    doThrow( failure ).when( aliveAccessors[i] ).validateBeforeCommit( ArgumentMatchers.any( Value[].class ) );
                }
                else
                {
                    doAnswer( invocation -> null ).when( aliveAccessors[i] ).validateBeforeCommit( any( Value[].class ) );
                }
            }

            // when
            try
            {
                fusionIndexAccessor.validateBeforeCommit( new Value[] {stringValue( "something" )} );
            }
            catch ( IllegalArgumentException e )
            {
                // then
                assertSame( failure, e );
            }
        }
    }

    @Test
    public void shouldSucceedValueValidationIfAllSucceed()
    {
        // when
        fusionIndexAccessor.validateBeforeCommit( new Value[] {stringValue( "test value" )} );

        // then no exception was thrown
    }

    @Test
    public void shouldInstantiateReadersLazily()
    {
        // when getting a new reader, no part-reader should be instantiated
        IndexReader fusionReader = fusionIndexAccessor.newReader();
        for ( int j = 0; j < aliveAccessors.length; j++ )
        {
            // then
            verifyNoMoreInteractions( aliveAccessors[j] );
        }
    }

    @Test
    public void shouldInstantiateUpdatersLazily()
    {
        // when getting a new reader, no part-reader should be instantiated
        IndexUpdater updater = fusionIndexAccessor.newUpdater( IndexUpdateMode.ONLINE );
        for ( int j = 0; j < aliveAccessors.length; j++ )
        {
            // then
            verifyNoMoreInteractions( aliveAccessors[j] );
        }
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
            mockSingleAllEntriesReader( aliveAccessors[i], entries[i] );
        }
    }
}
