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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.ArrayUtil.without;
import static org.neo4j.internal.schema.IndexProviderDescriptor.UNDECIDED;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.fill;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( RandomExtension.class )
abstract class FusionIndexAccessorTest
{
    private static final long indexId = 0;
    private FusionIndexAccessor fusionIndexAccessor;
    private EnumMap<IndexSlot,IndexAccessor> accessors;
    private IndexAccessor[] aliveAccessors;
    private IndexDescriptor indexDescriptor =
            IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 42 ) ).withName( "index" ).materialise( indexId );
    private FileSystemAbstraction fs;
    private IndexDirectoryStructure directoryStructure;

    @Inject
    private RandomRule random;
    private final FusionVersion fusionVersion;

    FusionIndexAccessorTest( FusionVersion fusionVersion )
    {
        this.fusionVersion = fusionVersion;
    }

    @BeforeEach
    void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        IndexSlot[] activeSlots = fusionVersion.aliveSlots();
        accessors = new EnumMap<>( IndexSlot.class );
        fill( accessors, IndexAccessor.EMPTY );
        aliveAccessors = new IndexAccessor[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexAccessor mock = mock( IndexAccessor.class );
            aliveAccessors[i] = mock;
            switch ( activeSlots[i] )
            {
            case GENERIC:
                accessors.put( GENERIC, mock );
                break;
            case LUCENE:
                accessors.put( LUCENE, mock );
                break;
            default:
                throw new RuntimeException();
            }
        }
        SlotSelector slotSelector = fusionVersion.slotSelector();
        InstanceSelector<IndexAccessor> instanceSelector = new InstanceSelector<>( accessors );
        fs = mock( FileSystemAbstraction.class );
        directoryStructure = directoriesByProvider( new File( "storeDir" ) ).forProvider( UNDECIDED );
        fusionIndexAccessor = new FusionIndexAccessor( slotSelector, instanceSelector, indexDescriptor, fs, directoryStructure );
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
    void dropMustDropAll() throws IOException
    {
        // when
        // ... all drop successful
        fusionIndexAccessor.drop();

        // then
        for ( IndexAccessor accessor : aliveAccessors )
        {
            verify( accessor ).drop();
        }
        verify( fs ).deleteRecursively( directoryStructure.directoryForIndex( indexId ) );
    }

    @Test
    void dropMustThrowIfDropAnyFail()
    {
        for ( IndexAccessor accessor : aliveAccessors )
        {
            // when
            verifyFailOnSingleDropFailure( accessor, fusionIndexAccessor );
        }
    }

    @Test
    void fusionIndexIsDirtyWhenAnyIsDirty()
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

    private static void verifyFailOnSingleDropFailure( IndexAccessor failingAccessor, FusionIndexAccessor fusionIndexAccessor )
    {
        UncheckedIOException expectedFailure = new UncheckedIOException( new IOException( "fail" ) );
        doThrow( expectedFailure ).when( failingAccessor ).drop();

        var e = assertThrows( UncheckedIOException.class, fusionIndexAccessor::drop );
        assertSame( expectedFailure, e );
        doAnswer( invocation -> null ).when( failingAccessor ).drop();
    }

    @Test
    void dropMustThrowIfAllFail()
    {
        // given
        List<UncheckedIOException> exceptions = new ArrayList<>();
        for ( IndexAccessor indexAccessor : aliveAccessors )
        {
            UncheckedIOException exception = new UncheckedIOException( new IOException( indexAccessor.getClass().getSimpleName() + " fail" ) );
            exceptions.add( exception );
            doThrow( exception ).when( indexAccessor ).drop();
        }

        var e = assertThrows( UncheckedIOException.class, () -> fusionIndexAccessor.drop() );
        assertThat( exceptions, hasItem( e ) );
    }

    /* close */

    @Test
    void closeMustCloseAll()
    {
        // when
        // ... all close successful
        fusionIndexAccessor.close();

        // then
        for ( IndexAccessor accessor : aliveAccessors )
        {
            verify( accessor ).close();
        }
    }

    @Test
    void closeMustThrowIfOneThrow() throws Exception
    {
        //noinspection ForLoopReplaceableByForEach - aliveAccessors is updated in initiateMocks()
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            IndexAccessor accessor = aliveAccessors[i];
            verifyFusionCloseThrowOnSingleCloseThrow( accessor, fusionIndexAccessor );
            initiateMocks();
        }
    }

    @Test
    void closeMustCloseOthersIfOneThrow() throws Exception
    {
        //noinspection ForLoopReplaceableByForEach - aliveAccessors is updated in initiateMocks()
        for ( int i = 0; i < aliveAccessors.length; i++ )
        {
            IndexAccessor accessor = aliveAccessors[i];
            verifyOtherIsClosedOnSingleThrow( accessor, fusionIndexAccessor, without( aliveAccessors, accessor ) );
            initiateMocks();
        }
    }

    @Test
    void closeMustThrowIfAllFail() throws Exception
    {
        verifyFusionCloseThrowIfAllThrow( fusionIndexAccessor, aliveAccessors );
    }

    // newAllEntriesReader

    @Test
    void allEntriesReaderMustCombineResultFromAll()
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
    void allEntriesReaderMustCombineResultFromAllEmpty()
    {
        // given
        List<Long>[] ids = new List[aliveAccessors.length];
        Arrays.fill( ids, Collections.emptyList() );
        mockAllEntriesReaders( ids );

        // when
        Set<Long> result = Iterables.asSet( fusionIndexAccessor.newAllEntriesReader() );

        // then
        assertTrue( result.isEmpty() );
    }

    @Test
    void allEntriesReaderMustCombineResultFromAllAccessors()
    {
        // given
        List<Long>[] parts = new List[aliveAccessors.length];
        Arrays.fill( parts, new ArrayList<>() );
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
    void allEntriesReaderMustCloseAll() throws Exception
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
            verify( allEntriesReader ).close();
        }
    }

    @Test
    void allEntriesReaderMustCloseOthersIfOneThrow() throws Exception
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
    void allEntriesReaderMustThrowIfOneThrow() throws Exception
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
    void allEntriesReaderMustReportUnknownMaxCountIfAnyReportUnknownMaxCount()
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
    void allEntriesReaderMustReportFusionMaxCountOfAll()
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
    void shouldFailValueValidationIfAnyPartFail()
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
    void shouldSucceedValueValidationIfAllSucceed()
    {
        // when
        fusionIndexAccessor.validateBeforeCommit( new Value[] {stringValue( "test value" )} );

        // then no exception was thrown
    }

    @Test
    void shouldInstantiateReadersLazily()
    {
        // when getting a new reader, no part-reader should be instantiated
        IndexReader fusionReader = fusionIndexAccessor.newReader();
        for ( IndexAccessor aliveAccessor : aliveAccessors )
        {
            // then
            verifyNoMoreInteractions( aliveAccessor );
        }
    }

    @Test
    void shouldInstantiateUpdatersLazily()
    {
        // when getting a new reader, no part-reader should be instantiated
        IndexUpdater updater = fusionIndexAccessor.newUpdater( IndexUpdateMode.ONLINE );
        for ( IndexAccessor aliveAccessor : aliveAccessors )
        {
            // then
            verifyNoMoreInteractions( aliveAccessor );
        }
    }

    private static void assertResultContainsAll( Set<Long> result, List<Long> expectedEntries )
    {
        for ( long expectedEntry : expectedEntries )
        {
            assertTrue( result.contains( expectedEntry ), "Expected to contain " + expectedEntry + ", but was " + result );
        }
    }

    private static BoundedIterable<Long> mockSingleAllEntriesReader( IndexAccessor targetAccessor, List<Long> entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReader( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
        return allEntriesReader;
    }

    private static BoundedIterable<Long> mockedAllEntriesReader( List<Long> entries )
    {
        return mockedAllEntriesReader( true, entries );
    }

    private static void mockSingleAllEntriesReaderWithUnknownMaxCount( IndexAccessor targetAccessor, List<Long> entries )
    {
        BoundedIterable<Long> allEntriesReader = mockedAllEntriesReaderUnknownMaxCount( entries );
        when( targetAccessor.newAllEntriesReader() ).thenReturn( allEntriesReader );
    }

    private static BoundedIterable<Long> mockedAllEntriesReaderUnknownMaxCount( List<Long> entries )
    {
        return mockedAllEntriesReader( false, entries );
    }

    private static BoundedIterable<Long> mockedAllEntriesReader( boolean knownMaxCount, List<Long> entries )
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
