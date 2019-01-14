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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.iterator.LongIterator;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.storageengine.api.schema.SimpleNodeValueClient;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.Predicates.in;
import static org.neo4j.helpers.collection.Iterables.asUniqueSet;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;
import static org.neo4j.values.storable.Values.of;

/**
 * Tests for
 * <ul>
 * <li>{@link NumberIndexAccessor}</li>
 * <li>{@link NativeIndexUpdater}</li>
 * <li>{@link NumberIndexReader}</li>
 * </ul>
 */
public abstract class NativeIndexAccessorTests<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends NativeIndexTestUtil<KEY,VALUE>
{
    private NativeIndexAccessor<KEY,VALUE> accessor;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setupAccessor() throws IOException
    {
        accessor = makeAccessor();
    }

    abstract NativeIndexAccessor<KEY,VALUE> makeAccessor() throws IOException;

    abstract IndexCapability indexCapability();

    @After
    public void closeAccessor()
    {
        accessor.close();
    }

    // UPDATER

    @Test
    public void shouldHandleCloseWithoutCallsToProcess() throws Exception
    {
        // given
        IndexUpdater updater = accessor.newUpdater( ONLINE );

        // when
        updater.close();

        // then
        // ... should be fine
    }

    @Test
    public void processMustThrowAfterClose() throws Exception
    {
        // given
        IndexUpdater updater = accessor.newUpdater( ONLINE );
        updater.close();

        // then
        expected.expect( IllegalStateException.class );

        // when
        updater.process( simpleUpdate() );
    }

    @Test
    public void shouldIndexAdd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
        {
            // when
            processAll( updater, updates );
        }

        // then
        forceAndCloseAccessor();
        verifyUpdates( updates );
    }

    @Test
    public void shouldIndexChange() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );
        Iterator<IndexEntryUpdate<IndexDescriptor>> generator = filter( skipExisting( updates ), valueCreatorUtil.randomUpdateGenerator( random ) );

        for ( int i = 0; i < updates.length; i++ )
        {
            IndexEntryUpdate<IndexDescriptor> update = updates[i];
            Value newValue = generator.next().values()[0];
            updates[i] = change( update.getEntityId(), indexDescriptor, update.values()[0], newValue );
        }

        // when
        processAll( updates );

        // then
        forceAndCloseAccessor();
        verifyUpdates( updates );
    }

    @Test
    public void shouldIndexRemove() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        for ( int i = 0; i < updates.length; i++ )
        {
            // when
            IndexEntryUpdate<IndexDescriptor> update = updates[i];
            IndexEntryUpdate<IndexDescriptor> remove = remove( update.getEntityId(), indexDescriptor, update.values() );
            processAll( remove );
            forceAndCloseAccessor();

            // then
            verifyUpdates( Arrays.copyOfRange( updates, i + 1, updates.length ) );
            setupAccessor();
        }
    }

    @Test
    public void shouldHandleRandomUpdates() throws Exception
    {
        // given
        Set<IndexEntryUpdate<IndexDescriptor>> expectedData = new HashSet<>();
        Iterator<IndexEntryUpdate<IndexDescriptor>> newDataGenerator = valueCreatorUtil.randomUpdateGenerator( random );

        // when
        int rounds = 50;
        for ( int round = 0; round < rounds; round++ )
        {
            // generate a batch of updates (add, change, remove)
            IndexEntryUpdate<IndexDescriptor>[] batch =
                    generateRandomUpdates( expectedData, newDataGenerator, random.nextInt( 5, 20 ), (float) round / rounds * 2 );
            // apply to tree
            processAll( batch );
            // apply to expectedData
            applyUpdatesToExpectedData( expectedData, batch );
            // verifyUpdates
            forceAndCloseAccessor();
            //noinspection unchecked
            verifyUpdates( expectedData.toArray( new IndexEntryUpdate[0] ) );
            setupAccessor();
        }
    }

    // === READER ===

    @Test
    public void shouldReturnZeroCountForEmptyIndex()
    {
        // given
        try ( IndexReader reader = accessor.newReader() )
        {
            // when
            IndexEntryUpdate<IndexDescriptor> update = valueCreatorUtil.randomUpdateGenerator( random ).next();
            long count = reader.countIndexedNodes( 123, valueCreatorUtil.indexDescriptor.properties(), update.values()[0] );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountOneForExistingData() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        // when
        try ( IndexReader reader = accessor.newReader() )
        {
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                long count = reader.countIndexedNodes( update.getEntityId(), valueCreatorUtil.indexDescriptor.properties(), update.values() );

                // then
                assertEquals( 1, count );
            }

            // and when
            Iterator<IndexEntryUpdate<IndexDescriptor>> generator = filter( skipExisting( updates ), valueCreatorUtil.randomUpdateGenerator( random ) );
            long count = reader.countIndexedNodes( 123, valueCreatorUtil.indexDescriptor.properties(), generator.next().values()[0] );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountZeroForMismatchingData() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();

        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            int[] propKeys = valueCreatorUtil.indexDescriptor.properties();
            long countWithMismatchingData = reader.countIndexedNodes( update.getEntityId() + 1, propKeys, update.values() );
            long countWithNonExistentEntityId = reader.countIndexedNodes( NON_EXISTENT_ENTITY_ID, propKeys, update.values() );
            long countWithNonExistentValue = reader.countIndexedNodes( update.getEntityId(), propKeys, generateUniqueValue( updates ) );

            // then
            assertEquals( 0, countWithMismatchingData );
            assertEquals( 0, countWithNonExistentEntityId );
            assertEquals( 0, countWithNonExistentValue );
        }
    }

    @Test
    public void shouldReturnAllEntriesForExistsPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader, IndexQuery.exists( 0 ) );

        // then
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnNoEntriesForExistsPredicateForEmptyIndex() throws Exception
    {
        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader, IndexQuery.exists( 0 ) );

        // then
        long[] actual = PrimitiveLongCollections.asArray( result );
        assertEquals( 0, actual.length );
    }

    @Test
    public void shouldReturnMatchingEntriesForExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            Value value = update.values()[0];
            LongIterator result = query( reader, IndexQuery.exact( 0, value ) );
            assertEntityIdHits( extractEntityIds( updates, in( value ) ), result );
        }
    }

    @Test
    public void shouldReturnNoEntriesForMismatchingExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        Object value = generateUniqueValue( updates );
        LongIterator result = query( reader, IndexQuery.exact( 0, value ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader,
                valueCreatorUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[updates.length - 1] ), false ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOf( updates, updates.length - 1 ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd( updates );
    }

    void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd( IndexEntryUpdate<IndexDescriptor>[] updates )
            throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader,
                valueCreatorUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[updates.length - 1] ), true ) );
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader,
                valueCreatorUtil.rangeQuery( valueOf( updates[0] ), false, valueOf( updates[updates.length - 1] ), false ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOfRange( updates, 1, updates.length - 1 ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader,
                valueCreatorUtil.rangeQuery( valueOf( updates[0] ), false, valueOf( updates[updates.length - 1] ), true ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOfRange( updates, 1, updates.length ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnNoEntriesForRangePredicateOutsideAnyMatch() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        valueCreatorUtil.sort( updates );
        processAll( updates[0], updates[1], updates[updates.length - 1], updates[updates.length - 2] );

        // when
        IndexReader reader = accessor.newReader();
        LongIterator result = query( reader,
                valueCreatorUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[updates.length - 3] ), true ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test( timeout = 10_000L )
    public void mustHandleNestedQueries() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        mustHandleNestedQueries( updates );
    }

    void mustHandleNestedQueries( IndexEntryUpdate<IndexDescriptor>[] updates ) throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();

        IndexQuery outerQuery = valueCreatorUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[3] ), true );
        IndexQuery innerQuery = valueCreatorUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[1] ), true );

        long[] expectedOuter = new long[]{entityIdOf( updates[2] ), entityIdOf( updates[3] )};
        long[] expectedInner = new long[]{entityIdOf( updates[0] ), entityIdOf( updates[1] )};

        LongIterator outerIter = query( reader, outerQuery );
        Collection<Long> outerResult = new ArrayList<>();
        while ( outerIter.hasNext() )
        {
            outerResult.add( outerIter.next() );
            LongIterator innerIter = query( reader, innerQuery );
            assertEntityIdHits( expectedInner, innerIter );
        }
        assertEntityIdHits( expectedOuter, outerResult );
    }

    @Test
    public void mustHandleMultipleNestedQueries() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        mustHandleMultipleNestedQueries( updates );
    }

    void mustHandleMultipleNestedQueries( IndexEntryUpdate<IndexDescriptor>[] updates )
            throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        processAll( updates );
        valueCreatorUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();

        IndexQuery query1 = valueCreatorUtil.rangeQuery( valueOf( updates[4] ), true, valueOf( updates[5] ), true );
        IndexQuery query2 = valueCreatorUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[3] ), true );
        IndexQuery query3 = valueCreatorUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[1] ), true );

        long[] expected1 = new long[]{entityIdOf( updates[4] ), entityIdOf( updates[5] )};
        long[] expected2 = new long[]{entityIdOf( updates[2] ), entityIdOf( updates[3] )};
        long[] expected3 = new long[]{entityIdOf( updates[0] ), entityIdOf( updates[1] )};

        Collection<Long> result1 = new ArrayList<>();
        LongIterator iter1 = query( reader, query1 );
        while ( iter1.hasNext() )
        {
            result1.add( iter1.next() );

            Collection<Long> result2 = new ArrayList<>();
            LongIterator iter2 = query( reader, query2 );
            while ( iter2.hasNext() )
            {
                result2.add( iter2.next() );

                Collection<Long> result3 = new ArrayList<>();
                LongIterator iter3 = query( reader, query3 );
                while ( iter3.hasNext() )
                {
                    result3.add( iter3.next() );
                }
                assertEntityIdHits( expected3, result3 );
            }
            assertEntityIdHits( expected2, result2 );
        }
        assertEntityIdHits( expected1, result1 );
    }

    private long entityIdOf( IndexEntryUpdate<IndexDescriptor> update )
    {
        return update.getEntityId();
    }

    @Test
    public void shouldHandleMultipleConsecutiveUpdaters() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();

        // when
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
            {
                updater.process( update );
            }
        }

        // then
        forceAndCloseAccessor();
        verifyUpdates( updates );
    }

    @Test
    public void requestForSecondUpdaterMustThrow() throws Exception
    {
        // given
        try ( IndexUpdater ignored = accessor.newUpdater( ONLINE ) )
        {
            // then
            expected.expect( IllegalStateException.class );

            // when
            accessor.newUpdater( ONLINE );
        }
    }

    @Test
    public void dropShouldDeleteAndCloseIndex()
    {
        // given
        assertFilePresent();

        // when
        accessor.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    public void forceShouldCheckpointTree() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] data = someUpdatesSingleType();
        processAll( data );

        // when
        accessor.force( IOLimiter.UNLIMITED );
        accessor.close();

        // then
        verifyUpdates( data );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void closeShouldCloseTreeWithoutCheckpoint() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] data = someUpdatesSingleType();
        processAll( data );

        // when
        accessor.close();

        // then
        verifyUpdates( new IndexEntryUpdate[0] );
    }

    @Test
    public void snapshotFilesShouldReturnIndexFile()
    {
        // when
        ResourceIterator<File> files = accessor.snapshotFiles();

        // then
        assertTrue( files.hasNext() );
        assertEquals( getIndexFile(), files.next() );
        assertFalse( files.hasNext() );
    }

    @Test
    public void shouldSampleIndex() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );
        try ( IndexReader reader = accessor.newReader() )
        {
            IndexSampler sampler = reader.createSampler();

            // when
            IndexSample sample = sampler.sampleIndex();

            // then
            assertEquals( updates.length, sample.indexSize() );
            assertEquals( updates.length, sample.sampleSize() );
            assertEquals( countUniqueValues( updates ), sample.uniqueValues() );
        }
    }

    @Test
    public void readingAfterDropShouldThrow()
    {
        // given
        accessor.drop();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newReader();
    }

    @Test
    public void writingAfterDropShouldThrow()
    {
        // given
        accessor.drop();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newUpdater( IndexUpdateMode.ONLINE );
    }

    @Test
    public void readingAfterCloseShouldThrow()
    {
        // given
        accessor.close();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newReader();
    }

    @Test
    public void writingAfterCloseShouldThrow()
    {
        // given
        accessor.close();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newUpdater( IndexUpdateMode.ONLINE );
    }

    @Test
    public void shouldSeeAllEntriesInAllEntriesReader() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll( updates );

        // when
        Set<Long> ids = asUniqueSet( accessor.newAllEntriesReader() );

        // then
        Set<Long> expectedIds = Stream.of( updates )
                .map( IndexEntryUpdate::getEntityId )
                .collect( Collectors.toCollection( HashSet::new ) );
        assertEquals( expectedIds, ids );
    }

    @Test
    public void shouldSeeNoEntriesInAllEntriesReaderOnEmptyIndex()
    {
        // when
        Set<Long> ids = asUniqueSet( accessor.newAllEntriesReader() );

        // then
        Set<Long> expectedIds = Collections.emptySet();
        assertEquals( expectedIds, ids );
    }

    @Test
    public void shouldNotSeeFilteredEntries() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates( supportedTypesExcludingNonOrderable() );
        processAll( updates );
        valueCreatorUtil.sort( updates );
        IndexReader reader = accessor.newReader();

        // when
        NodeValueIterator iter = new NodeValueIterator();
        IndexQuery.ExactPredicate filter = IndexQuery.exact( 0, valueOf( updates[1]) );
        IndexQuery rangeQuery = valueCreatorUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[2] ), true );
        IndexProgressor.NodeValueClient filterClient = filterClient( iter, filter );
        reader.query( filterClient, IndexOrder.NONE, false, rangeQuery );

        // then
        assertTrue( iter.hasNext() );
        assertEquals( entityIdOf( updates[1] ), iter.next() );
        assertFalse( iter.hasNext() );
    }

    // <READER ordering>

    @Test
    public void respectIndexOrder() throws Exception
    {
        // given
        int nUpdates = 10000;
        ValueType[] types = supportedTypesExcludingNonOrderable();
        Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator =
                valueCreatorUtil.randomUpdateGenerator( random, types );
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] someUpdates = new IndexEntryUpdate[nUpdates];
        for ( int i = 0; i < nUpdates; i++ )
        {
            someUpdates[i] = randomUpdateGenerator.next();
        }
        processAll( someUpdates );
        Value[] allValues = valueCreatorUtil.extractValuesFromUpdates( someUpdates );

        // when
        try ( IndexReader reader = accessor.newReader() )
        {
            ValueGroup valueGroup = random.among( allValues ).valueGroup();
            IndexQuery.RangePredicate<?> supportedQuery = IndexQuery.range( 0, valueGroup );

            IndexOrder[] supportedOrders = indexCapability().orderCapability( valueGroup.category() );
            for ( IndexOrder supportedOrder : supportedOrders )
            {
                if ( supportedOrder == IndexOrder.NONE )
                {
                    continue;
                }
                Value[] expectedValues = Arrays.stream( allValues )
                        .filter( v -> v.valueGroup() == valueGroup )
                        .toArray( Value[]::new );
                if ( supportedOrder == IndexOrder.ASCENDING )
                {
                    Arrays.sort( expectedValues, Values.COMPARATOR );
                }
                if ( supportedOrder == IndexOrder.DESCENDING )
                {
                    Arrays.sort( expectedValues, Values.COMPARATOR.reversed() );
                }

                SimpleNodeValueClient client = new SimpleNodeValueClient();
                reader.query( client, supportedOrder, true, supportedQuery );
                int i = 0;
                while ( client.next() )
                {
                    assertEquals( "values in order", expectedValues[i++], client.values[0] );
                }
                assertEquals( "found all values", i, expectedValues.length );
            }
        }
    }

    @Test
    public void throwForUnsupportedIndexOrder() throws Exception
    {
        // given
        // Unsupported index order for query
        try ( IndexReader reader = accessor.newReader() )
        {
            IndexOrder unsupportedOrder = IndexOrder.DESCENDING;
            IndexQuery.ExactPredicate unsupportedQuery = IndexQuery.exact( 0, PointValue.MAX_VALUE ); // <- Any spatial value would do

            // then
            expected.expect( UnsupportedOperationException.class );
            expected.expectMessage( CoreMatchers.allOf(
                    CoreMatchers.containsString( "unsupported order" ),
                    CoreMatchers.containsString( unsupportedOrder.toString() ),
                    CoreMatchers.containsString( unsupportedQuery.toString() ) ) );

            // when
            reader.query( new SimpleNodeValueClient(), unsupportedOrder, false, unsupportedQuery );
        }
    }

    @Test
    public void getValues() throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        // given
        int nUpdates = 10000;
        Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator = valueCreatorUtil.randomUpdateGenerator( random );
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] someUpdates = new IndexEntryUpdate[nUpdates];
        for ( int i = 0; i < nUpdates; i++ )
        {
            someUpdates[i] = randomUpdateGenerator.next();
        }
        processAll( someUpdates );
        Value[] allValues = valueCreatorUtil.extractValuesFromUpdates( someUpdates );

        // Pick one out of all added values and do a range query for the value group of that value
        Value value = random.among( allValues );
        ValueGroup valueGroup = value.valueGroup();

        IndexValueCapability valueCapability = indexCapability().valueCapability( valueGroup.category() );
        if ( !valueCapability.equals( IndexValueCapability.YES ) )
        {
            // We don't need to do this test
            return;
        }

        IndexQuery.RangePredicate<?> supportedQuery;
        List<Value> expectedValues;
        if ( Values.isGeometryValue( value ) )
        {
            // Unless it's a point value in which case we query for the specific coordinate reference system instead
            CoordinateReferenceSystem crs = ((PointValue) value).getCoordinateReferenceSystem();
            supportedQuery = IndexQuery.range( 0, crs );
            expectedValues = Arrays.stream( allValues )
                    .filter( v -> v.valueGroup() == ValueGroup.GEOMETRY )
                    .filter( v -> ((PointValue) v).getCoordinateReferenceSystem() == crs )
                    .collect( Collectors.toList() );
        }
        else
        {
            supportedQuery = IndexQuery.range( 0, valueGroup );
            expectedValues = Arrays.stream( allValues )
                    .filter( v -> v.valueGroup() == valueGroup )
                    .collect( Collectors.toList() );
        }

        // when
        try ( IndexReader reader = accessor.newReader() )
        {
                SimpleNodeValueClient client = new SimpleNodeValueClient();
                reader.query( client, IndexOrder.NONE, true, supportedQuery );

                // then
                while ( client.next() )
                {
                    Value foundValue = client.values[0];
                    assertTrue( "found value that was not expected " + foundValue, expectedValues.remove( foundValue ) );
                }
                assertThat( "did not find all expected values", expectedValues.size(), CoreMatchers.is( 0 ) );
        }
    }

    // </READER ordering>

    private Value generateUniqueValue( IndexEntryUpdate<IndexDescriptor>[] updates )
    {
        return filter( skipExisting( updates ), valueCreatorUtil.randomUpdateGenerator( random ) ).next().values()[0];
    }

    private static Predicate<IndexEntryUpdate<IndexDescriptor>> skipExisting( IndexEntryUpdate<IndexDescriptor>[] existing )
    {
        return update ->
        {
            for ( IndexEntryUpdate<IndexDescriptor> e : existing )
            {
                if ( Arrays.equals( e.values(), update.values() ) )
                {
                    return false;
                }
            }
            return true;
        };
    }

    private Value valueOf( IndexEntryUpdate<IndexDescriptor> update )
    {
        return update.values()[0];
    }

    private IndexProgressor.NodeValueClient filterClient( final NodeValueIterator iter, final IndexQuery filter )
    {
        return new IndexProgressor.NodeValueClient()
        {
            @Override
            public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues )
            {
                iter.initialize( descriptor, progressor, query, indexOrder, needsValues );
            }

            @Override
            public boolean acceptNode( long reference, Value... values )
            {
                //noinspection SimplifiableIfStatement
                if ( values.length > 1 )
                {
                    return false;
                }
                return filter.acceptsValue( values[0] ) && iter.acceptNode( reference, values );
            }

            @Override
            public boolean needsValues()
            {
                return true;
            }
        };
    }

    private LongIterator query( IndexReader reader, IndexQuery query ) throws IndexNotApplicableKernelException
    {
        NodeValueIterator client = new NodeValueIterator();
        reader.query( client, IndexOrder.NONE, false, query );
        return client;
    }

    private void assertEntityIdHits( long[] expected, LongIterator result )
    {
        long[] actual = PrimitiveLongCollections.asArray( result );
        assertSameContent( expected, actual );
    }

    private void assertEntityIdHits( long[] expected, Collection<Long> result )
    {
        long[] actual = new long[result.size()];
        int index = 0;
        for ( Long aLong : result )
        {
            actual[index++] = aLong;
        }
        assertSameContent( expected, actual );
    }

    private void assertSameContent( long[] expected, long[] actual )
    {
        Arrays.sort( actual );
        Arrays.sort( expected );
        assertArrayEquals( format( "Expected arrays to be equal but wasn't.%nexpected:%s%n  actual:%s%n",
                Arrays.toString( expected ), Arrays.toString( actual ) ), expected, actual );
    }

    private long[] extractEntityIds( IndexEntryUpdate<?>[] updates, Predicate<Value> valueFilter )
    {
        long[] entityIds = new long[updates.length];
        int cursor = 0;
        for ( IndexEntryUpdate<?> update : updates )
        {
            if ( valueFilter.test( update.values()[0] ) )
            {
                entityIds[cursor++] = update.getEntityId();
            }
        }
        return Arrays.copyOf( entityIds, cursor );
    }

    private void applyUpdatesToExpectedData( Set<IndexEntryUpdate<IndexDescriptor>> expectedData,
            IndexEntryUpdate<IndexDescriptor>[] batch )
    {
        for ( IndexEntryUpdate<IndexDescriptor> update : batch )
        {
            IndexEntryUpdate<IndexDescriptor> addition = null;
            IndexEntryUpdate<IndexDescriptor> removal = null;
            switch ( update.updateMode() )
            {
            case ADDED:
                addition = valueCreatorUtil.add( update.getEntityId(), update.values()[0] );
                break;
            case CHANGED:
                addition = valueCreatorUtil.add( update.getEntityId(), update.values()[0] );
                removal = valueCreatorUtil.add( update.getEntityId(), update.beforeValues()[0] );
                break;
            case REMOVED:
                removal = valueCreatorUtil.add( update.getEntityId(), update.values()[0] );
                break;
            default:
                throw new IllegalArgumentException( update.updateMode().name() );
            }

            if ( removal != null )
            {
                expectedData.remove( removal );
            }
            if ( addition != null )
            {
                expectedData.add( addition );
            }
        }
    }

    private IndexEntryUpdate<IndexDescriptor>[] generateRandomUpdates(
            Set<IndexEntryUpdate<IndexDescriptor>> expectedData,
            Iterator<IndexEntryUpdate<IndexDescriptor>> newDataGenerator, int count, float removeFactor )
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = new IndexEntryUpdate[count];
        float addChangeRatio = 0.5f;
        for ( int i = 0; i < count; i++ )
        {
            float factor = random.nextFloat();
            if ( !expectedData.isEmpty() && factor < removeFactor )
            {
                // remove something
                IndexEntryUpdate<IndexDescriptor> toRemove = selectRandomItem( expectedData );
                updates[i] = remove( toRemove.getEntityId(), indexDescriptor, toRemove.values() );
            }
            else if ( !expectedData.isEmpty() && factor < (1 - removeFactor) * addChangeRatio )
            {
                // change
                IndexEntryUpdate<IndexDescriptor> toChange = selectRandomItem( expectedData );
                // use the data generator to generate values, even if the whole update as such won't be used
                IndexEntryUpdate<IndexDescriptor> updateContainingValue = newDataGenerator.next();
                updates[i] = change( toChange.getEntityId(), indexDescriptor, toChange.values(),
                        updateContainingValue.values() );
            }
            else
            {
                // add
                updates[i] = newDataGenerator.next();
            }
        }
        return updates;
    }

    @SuppressWarnings( "unchecked" )
    private IndexEntryUpdate<IndexDescriptor> selectRandomItem( Set<IndexEntryUpdate<IndexDescriptor>> expectedData )
    {
        return expectedData.toArray( new IndexEntryUpdate[0] )[random.nextInt( expectedData.size() )];
    }

    @SafeVarargs
    private final void processAll( IndexEntryUpdate<IndexDescriptor>... updates )
            throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
        {
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }
    }

    private void forceAndCloseAccessor()
    {
        accessor.force( IOLimiter.UNLIMITED );
        closeAccessor();
    }

    private void processAll( IndexUpdater updater, IndexEntryUpdate<IndexDescriptor>[] updates )
            throws IndexEntryConflictException
    {
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            updater.process( update );
        }
    }

    private IndexEntryUpdate<IndexDescriptor> simpleUpdate()
    {
        return IndexEntryUpdate.add( 0, indexDescriptor, of( 0 ) );
    }

    private IndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleType()
    {
        ValueType type = random.randomValues().among( valueCreatorUtil.supportedTypes() );
        return valueCreatorUtil.someUpdates( random, new ValueType[]{type}, true );
    }

    private IndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleTypeNoDuplicates()
    {
        return someUpdatesSingleTypeNoDuplicates( valueCreatorUtil.supportedTypes() );
    }

    private IndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleTypeNoDuplicates( ValueType[] types )
    {
        ValueType type;
        do
        {
            // Can not generate enough unique values of boolean
            type = random.randomValues().among( types );
        }
        while ( type == ValueType.BOOLEAN );
        return valueCreatorUtil.someUpdates( random, new ValueType[]{type}, false );
    }

    private ValueType[] supportedTypesExcludingNonOrderable()
    {
        return RandomValues.excluding( valueCreatorUtil.supportedTypes(),
                t -> t.valueGroup == ValueGroup.GEOMETRY ||
                        t.valueGroup == ValueGroup.GEOMETRY_ARRAY ||
                        t == ValueType.STRING ||
                        t == ValueType.STRING_ARRAY );
    }

    // TODO: multiple query predicates... actually Lucene SimpleIndexReader only supports single predicate
    //       so perhaps we should wait with this until we know exactly how this works and which combinations
    //       that should be supported/optimized for.
}
