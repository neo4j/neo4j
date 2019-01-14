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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.Predicates.in;
import static org.neo4j.helpers.collection.Iterables.asUniqueSet;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.LayoutTestUtil.countUniqueValues;
import static org.neo4j.values.storable.Values.of;

/**
 * Tests for
 * <ul>
 * <li>{@link NumberSchemaIndexAccessor}</li>
 * <li>{@link NativeSchemaIndexUpdater}</li>
 * <li>{@link NumberSchemaIndexReader}</li>
 * </ul>
 */
public abstract class NativeSchemaIndexAccessorTest<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue>
        extends NativeSchemaIndexTestUtil<KEY,VALUE>
{
    NativeSchemaIndexAccessor<KEY,VALUE> accessor;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setupAccessor() throws IOException
    {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        accessor = makeAccessorWithSamplingConfig( samplingConfig );
    }

    abstract NativeSchemaIndexAccessor<KEY,VALUE> makeAccessorWithSamplingConfig( IndexSamplingConfig samplingConfig ) throws IOException;

    @After
    public void closeAccessor() throws IOException
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );
        Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> generator = filter( skipExisting( updates ), layoutUtil.randomUpdateGenerator( random ) );

        for ( int i = 0; i < updates.length; i++ )
        {
            IndexEntryUpdate<SchemaIndexDescriptor> update = updates[i];
            Value newValue = generator.next().values()[0];
            updates[i] = change( update.getEntityId(), schemaIndexDescriptor, update.values()[0], newValue );
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        for ( int i = 0; i < updates.length; i++ )
        {
            // when
            IndexEntryUpdate<SchemaIndexDescriptor> update = updates[i];
            IndexEntryUpdate<SchemaIndexDescriptor> remove = remove( update.getEntityId(),
                    schemaIndexDescriptor, update.values() );
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
        Set<IndexEntryUpdate<SchemaIndexDescriptor>> expectedData = new HashSet<>();
        Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> newDataGenerator = layoutUtil.randomUpdateGenerator( random );

        // when
        int rounds = 50;
        for ( int round = 0; round < rounds; round++ )
        {
            // generate a batch of updates (add, change, remove)
            IndexEntryUpdate<SchemaIndexDescriptor>[] batch = generateRandomUpdates( expectedData, newDataGenerator,
                    random.nextInt( 5, 20 ), (float) round / rounds * 2 );
            // apply to tree
            processAll( batch );
            // apply to expectedData
            applyUpdatesToExpectedData( expectedData, batch );
            // verifyUpdates
            forceAndCloseAccessor();
            verifyUpdates( expectedData.toArray( new IndexEntryUpdate[expectedData.size()] ) );
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
            IndexEntryUpdate<SchemaIndexDescriptor> update = layoutUtil.randomUpdateGenerator( random ).next();
            long count = reader.countIndexedNodes( 123, update.values()[0] );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountOneForExistingData() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        try ( IndexReader reader = accessor.newReader() )
        {
            for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
            {
                long count = reader.countIndexedNodes( update.getEntityId(), update.values() );

                // then
                assertEquals( 1, count );
            }

            // and when
            Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> generator = filter( skipExisting( updates ), layoutUtil.randomUpdateGenerator( random ) );
            long count = reader.countIndexedNodes( 123, generator.next().values()[0] );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountZeroForMismatchingData() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();

        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
        {
            long countWithMismatchingData = reader.countIndexedNodes( update.getEntityId() + 1, update.values() );
            long countWithNonExistentEntityId = reader.countIndexedNodes( NON_EXISTENT_ENTITY_ID, update.values() );
            long countWithNonExistentValue = reader.countIndexedNodes( update.getEntityId(), generateUniqueValue( updates ) );

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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader, IndexQuery.exists( 0 ) );

        // then
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnNoEntriesForExistsPredicateForEmptyIndex() throws Exception
    {
        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader, IndexQuery.exists( 0 ) );

        // then
        long[] actual = PrimitiveLongCollections.asArray( result );
        assertEquals( 0, actual.length );
    }

    @Test
    public void shouldReturnMatchingEntriesForExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
        {
            Value value = update.values()[0];
            PrimitiveLongIterator result = query( reader, IndexQuery.exact( 0, value ) );
            assertEntityIdHits( extractEntityIds( updates, in( value ) ), result );
        }
    }

    @Test
    public void shouldReturnNoEntriesForMismatchingExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        Object value = generateUniqueValue( updates );
        PrimitiveLongIterator result = query( reader, IndexQuery.exact( 0, value ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesNoDuplicateValues();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader,
                layoutUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[updates.length - 1] ), false ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOf( updates, updates.length - 1 ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesNoDuplicateValues();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader,
                layoutUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[updates.length - 1] ), true ) );
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesNoDuplicateValues();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader,
                layoutUtil.rangeQuery( valueOf( updates[0] ), false, valueOf( updates[updates.length - 1] ), false ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOfRange( updates, 1, updates.length - 1 ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesNoDuplicateValues();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader,
                layoutUtil.rangeQuery( valueOf( updates[0] ), false, valueOf( updates[updates.length - 1] ), true ) );
        assertEntityIdHits( extractEntityIds( Arrays.copyOfRange( updates, 1, updates.length ), alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnNoEntriesForRangePredicateOutsideAnyMatch() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        layoutUtil.sort( updates );
        processAll( updates[0], updates[1], updates[updates.length - 1], updates[updates.length - 2] );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = query( reader,
                layoutUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[updates.length - 3] ), true ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test( timeout = 10_000L )
    public void mustHandleNestedQueries() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();

        IndexQuery outerQuery = layoutUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[3] ), true );
        IndexQuery innerQuery = layoutUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[1] ), true );

        long[] expectedOuter = new long[]{entityIdOf( updates[2] ), entityIdOf( updates[3] )};
        long[] expectedInner = new long[]{entityIdOf( updates[0] ), entityIdOf( updates[1] )};

        PrimitiveLongIterator outerIter = query( reader, outerQuery );
        Collection<Long> outerResult = new ArrayList<>();
        while ( outerIter.hasNext() )
        {
            outerResult.add( outerIter.next() );
            PrimitiveLongIterator innerIter = query( reader, innerQuery );
            assertEntityIdHits( expectedInner, innerIter );
        }
        assertEntityIdHits( expectedOuter, outerResult );
    }

    @Test
    public void mustHandleMultipleNestedQueries() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );
        layoutUtil.sort( updates );

        // when
        IndexReader reader = accessor.newReader();

        IndexQuery query1 = layoutUtil.rangeQuery( valueOf( updates[4] ), true, valueOf( updates[5] ), true );
        IndexQuery query2 = layoutUtil.rangeQuery( valueOf( updates[2] ), true, valueOf( updates[3] ), true );
        IndexQuery query3 = layoutUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[1] ), true );

        long[] expected1 = new long[]{entityIdOf( updates[4] ), entityIdOf( updates[5] )};
        long[] expected2 = new long[]{entityIdOf( updates[2] ), entityIdOf( updates[3] )};
        long[] expected3 = new long[]{entityIdOf( updates[0] ), entityIdOf( updates[1] )};

        Collection<Long> result1 = new ArrayList<>();
        PrimitiveLongIterator iter1 = query( reader, query1 );
        while ( iter1.hasNext() )
        {
            result1.add( iter1.next() );

            Collection<Long> result2 = new ArrayList<>();
            PrimitiveLongIterator iter2 = query( reader, query2 );
            while ( iter2.hasNext() )
            {
                result2.add( iter2.next() );

                Collection<Long> result3 = new ArrayList<>();
                PrimitiveLongIterator iter3 = query( reader, query3 );
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

    private long entityIdOf( IndexEntryUpdate<SchemaIndexDescriptor> update )
    {
        return update.getEntityId();
    }

    @Test
    public void shouldHandleMultipleConsecutiveUpdaters() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();

        // when
        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
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
    public void dropShouldDeleteAndCloseIndex() throws Exception
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] data = layoutUtil.someUpdates();
        processAll( data );

        // when
        accessor.force( IOLimiter.unlimited() );
        accessor.close();

        // then
        verifyUpdates( data );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void closeShouldCloseTreeWithoutCheckpoint() throws Exception
    {
        // given
        IndexEntryUpdate<SchemaIndexDescriptor>[] data = layoutUtil.someUpdates();
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
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
    public void readingAfterDropShouldThrow() throws Exception
    {
        // given
        accessor.drop();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newReader();
    }

    @Test
    public void writingAfterDropShouldThrow() throws Exception
    {
        // given
        accessor.drop();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newUpdater( IndexUpdateMode.ONLINE );
    }

    @Test
    public void readingAfterCloseShouldThrow() throws Exception
    {
        // given
        accessor.close();

        // then
        expected.expect( IllegalStateException.class );

        // when
        accessor.newReader();
    }

    @Test
    public void writingAfterCloseShouldThrow() throws Exception
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdates();
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
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = layoutUtil.someUpdatesNoDuplicateValues();
        processAll( updates );
        layoutUtil.sort( updates );
        IndexReader reader = accessor.newReader();

        // when
        NodeValueIterator iter = new NodeValueIterator();
        IndexQuery.ExactPredicate filter = IndexQuery.exact( 0, valueOf( updates[1]) );
        IndexQuery rangeQuery = layoutUtil.rangeQuery( valueOf( updates[0] ), true, valueOf( updates[2] ), true );
        IndexProgressor.NodeValueClient filterClient = filterClient( iter, filter );
        reader.query( filterClient, IndexOrder.NONE, rangeQuery );

        // then
        assertTrue( iter.hasNext() );
        assertEquals( entityIdOf( updates[1] ), iter.next() );
        assertFalse( iter.hasNext() );
    }

    private Value generateUniqueValue( IndexEntryUpdate<SchemaIndexDescriptor>[] updates )
    {
        return filter( skipExisting( updates ), layoutUtil.randomUpdateGenerator( random ) ).next().values()[0];
    }

    private static Predicate<IndexEntryUpdate<SchemaIndexDescriptor>> skipExisting( IndexEntryUpdate<SchemaIndexDescriptor>[] existing )
    {
        return update ->
        {
            for ( IndexEntryUpdate<SchemaIndexDescriptor> e : existing )
            {
                if ( Arrays.equals( e.values(), update.values() ) )
                {
                    return false;
                }
            }
            return true;
        };
    }

    private Value valueOf( IndexEntryUpdate<SchemaIndexDescriptor> update )
    {
        return update.values()[0];
    }

    private IndexProgressor.NodeValueClient filterClient( final NodeValueIterator iter, final IndexQuery.ExactPredicate filter )
    {
        return new IndexProgressor.NodeValueClient()
        {
            @Override
            public void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query )
            {
                iter.initialize( descriptor, progressor, query );
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

    private PrimitiveLongIterator query( IndexReader reader, IndexQuery query ) throws IndexNotApplicableKernelException
    {
        NodeValueIterator client = new NodeValueIterator();
        reader.query( client, IndexOrder.NONE, query );
        return client;
    }

    private void assertEntityIdHits( long[] expected, PrimitiveLongIterator result )
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

    private void applyUpdatesToExpectedData( Set<IndexEntryUpdate<SchemaIndexDescriptor>> expectedData,
            IndexEntryUpdate<SchemaIndexDescriptor>[] batch )
    {
        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : batch )
        {
            IndexEntryUpdate<SchemaIndexDescriptor> addition = null;
            IndexEntryUpdate<SchemaIndexDescriptor> removal = null;
            switch ( update.updateMode() )
            {
            case ADDED:
                addition = layoutUtil.add( update.getEntityId(), update.values()[0] );
                break;
            case CHANGED:
                addition = layoutUtil.add( update.getEntityId(), update.values()[0] );
                removal = layoutUtil.add( update.getEntityId(), update.beforeValues()[0] );
                break;
            case REMOVED:
                removal = layoutUtil.add( update.getEntityId(), update.values()[0] );
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

    private IndexEntryUpdate<SchemaIndexDescriptor>[] generateRandomUpdates(
            Set<IndexEntryUpdate<SchemaIndexDescriptor>> expectedData,
            Iterator<IndexEntryUpdate<SchemaIndexDescriptor>> newDataGenerator, int count, float removeFactor )
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<SchemaIndexDescriptor>[] updates = new IndexEntryUpdate[count];
        float addChangeRatio = 0.5f;
        for ( int i = 0; i < count; i++ )
        {
            float factor = random.nextFloat();
            if ( !expectedData.isEmpty() && factor < removeFactor )
            {
                // remove something
                IndexEntryUpdate<SchemaIndexDescriptor> toRemove = selectRandomItem( expectedData );
                updates[i] = remove( toRemove.getEntityId(), schemaIndexDescriptor, toRemove.values() );
            }
            else if ( !expectedData.isEmpty() && factor < (1 - removeFactor) * addChangeRatio )
            {
                // change
                IndexEntryUpdate<SchemaIndexDescriptor> toChange = selectRandomItem( expectedData );
                // use the data generator to generate values, even if the whole update as such won't be used
                IndexEntryUpdate<SchemaIndexDescriptor> updateContainingValue = newDataGenerator.next();
                updates[i] = change( toChange.getEntityId(), schemaIndexDescriptor, toChange.values(),
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
    private IndexEntryUpdate<SchemaIndexDescriptor> selectRandomItem( Set<IndexEntryUpdate<SchemaIndexDescriptor>> expectedData )
    {
        return expectedData.toArray( new IndexEntryUpdate[expectedData.size()] )[random.nextInt( expectedData.size() )];
    }

    @SafeVarargs
    final void processAll( IndexEntryUpdate<SchemaIndexDescriptor>... updates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
        {
            for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }
    }

    private void forceAndCloseAccessor() throws IOException
    {
        accessor.force( IOLimiter.unlimited() );
        closeAccessor();
    }

    private void processAll( IndexUpdater updater, IndexEntryUpdate<SchemaIndexDescriptor>[] updates )
            throws IOException, IndexEntryConflictException
    {
        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
        {
            updater.process( update );
        }
    }

    private IndexEntryUpdate<SchemaIndexDescriptor> simpleUpdate()
    {
        return IndexEntryUpdate.add( 0, schemaIndexDescriptor, of( 0 ) );
    }

    // TODO: multiple query predicates... actually Lucene SimpleIndexReader only supports single predicate
    //       so perhaps we should wait with this until we know exactly how this works and which combinations
    //       that should be supported/optimized for.
}
