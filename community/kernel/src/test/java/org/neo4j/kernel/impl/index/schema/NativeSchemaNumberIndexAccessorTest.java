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
package org.neo4j.kernel.impl.index.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.function.Predicates.all;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.Predicates.in;
import static org.neo4j.helpers.collection.Iterables.asUniqueSet;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.LayoutTestUtil.countUniqueValues;
import static org.neo4j.values.storable.Values.COMPARATOR;
import static org.neo4j.values.storable.Values.of;

/**
 * Tests for
 * <ul>
 * <li>{@link NativeSchemaNumberIndexAccessor}</li>
 * <li>{@link NativeSchemaNumberIndexUpdater}</li>
 * <li>{@link NativeSchemaNumberIndexReader}</li>
 * </ul>
 */
public abstract class NativeSchemaNumberIndexAccessorTest<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        extends SchemaNumberIndexTestUtil<KEY,VALUE>
{
    private NativeSchemaNumberIndexAccessor<KEY,VALUE> accessor;

    @Before
    public void setupAccessor() throws IOException
    {
        accessor = new NativeSchemaNumberIndexAccessor<>( pageCache, fs, indexFile, layout, IMMEDIATE, monitor, indexDescriptor, indexId );
    }

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

        // when
        try
        {
            updater.process( simpleUpdate() );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void shouldIndexAdd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
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
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        for ( int i = 0; i < updates.length; i++ )
        {
            IndexEntryUpdate<IndexDescriptor> update = updates[i];
            Object newValue;
            switch ( i % 3 )
            {
            case 0:
                newValue = NON_EXISTENT_VALUE + i;
                break;
            case 1:
                newValue = (float) NON_EXISTENT_VALUE + i;
                break;
            case 2:
                newValue = (double) NON_EXISTENT_VALUE + i;
                break;
            default:
                throw new IllegalArgumentException();
            }
            updates[i] = change( update.getEntityId(), indexDescriptor, update.values()[0], of( newValue ) );
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
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
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

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleRandomUpdates() throws Exception
    {
        // given
        Set<IndexEntryUpdate<IndexDescriptor>> expectedData = new HashSet<>();
        Iterator<IndexEntryUpdate<IndexDescriptor>> newDataGenerator = layoutUtil.randomUpdateGenerator( random );

        // when
        int rounds = 50;
        for ( int round = 0; round < rounds; round++ )
        {
            // generate a batch of updates (add, change, remove)
            IndexEntryUpdate<IndexDescriptor>[] batch = generateRandomUpdates( expectedData, newDataGenerator,
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
    public void shouldReturnZeroCountForEmptyIndex() throws Exception
    {
        // given
        try ( IndexReader reader = accessor.newReader() )
        {
            // when
            long count = reader.countIndexedNodes( 123, of( 456 ) );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountOneForExistingData() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        try ( IndexReader reader = accessor.newReader() )
        {
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                long count = reader.countIndexedNodes( update.getEntityId(), update.values() );

                // then
                assertEquals( 1, count );
            }

            // and when
            long count = reader.countIndexedNodes( 123, of( 456 ) );

            // then
            assertEquals( 0, count );
        }
    }

    @Test
    public void shouldReturnCountZeroForMismatchingData() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();

        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            long countWithMismatchingData = reader.countIndexedNodes( update.getEntityId() + 1, update.values() );
            long countWithNonExistentEntityId = reader.countIndexedNodes( NON_EXISTENT_ENTITY_ID, update.values() );
            long countWithNonExistentValue = reader.countIndexedNodes( update.getEntityId(), of( NON_EXISTENT_VALUE ) );

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
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query( IndexQuery.exists( 0 ) );

        // then
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnNoEntriesForExistsPredicateForEmptyIndex() throws Exception
    {
        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query( IndexQuery.exists( 0 ) );

        // then
        long[] actual = PrimitiveLongCollections.asArray( result );
        assertEquals( 0, actual.length );
    }

    @Test
    public void shouldReturnMatchingEntriesForExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            Value value = update.values()[0];
            PrimitiveLongIterator result = reader.query( IndexQuery.exact( 0, value ) );
            assertEntityIdHits( extractEntityIds( updates, in( value ) ), result );
        }
    }

    @Test
    public void shouldReturnNoEntriesForMismatchingExactPredicate() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        Object value = NON_EXISTENT_VALUE;
        PrimitiveLongIterator result = reader.query( IndexQuery.exact( 0, value ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query(
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, false ) );
        assertEntityIdHits( extractEntityIds( updates, lessThan( Double.POSITIVE_INFINITY ) ), result );
    }

    private static int compare( Value value, Number other )
    {
        return COMPARATOR.compare( value, Values.of( other ) );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query(
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true ) );
        assertEntityIdHits( extractEntityIds( updates, alwaysTrue() ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query(
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, false, Double.POSITIVE_INFINITY, false ) );
        assertEntityIdHits( extractEntityIds( updates,
                all( greaterThan( Double.NEGATIVE_INFINITY ), lessThan( Double.POSITIVE_INFINITY ) ) ), result );
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query(
                IndexQuery.range( 0, Double.NEGATIVE_INFINITY, false, Double.POSITIVE_INFINITY, true ) );
        assertEntityIdHits( extractEntityIds( updates,
                all( greaterThan( Double.NEGATIVE_INFINITY ), greaterThan( Double.NEGATIVE_INFINITY ) ) ), result );
    }

    @Test
    public void shouldReturnNoEntriesForRangePredicateOutsideAnyMatch() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        PrimitiveLongIterator result = reader.query(
                IndexQuery.range( 0, NON_EXISTENT_VALUE, true, NON_EXISTENT_VALUE + 10, true ) );
        assertEntityIdHits( EMPTY_LONG_ARRAY, result );
    }

    @Test
    public void shouldHandleMultipleConsecutiveUpdaters() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();

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
            // when
            try
            {
                accessor.newUpdater( ONLINE );
                fail( "Should have failed" );
            }
            catch ( IllegalStateException e )
            {
                // then good
            }
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
        IndexEntryUpdate<IndexDescriptor>[] data = layoutUtil.someUpdates();
        processAll( data );

        // when
        accessor.force();
        accessor.close();

        // then
        verifyUpdates( data );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void closeShouldCloseTreeWithoutCheckpoint() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] data = layoutUtil.someUpdates();
        processAll( data );

        // when
        accessor.close();

        // then
        verifyUpdates( new IndexEntryUpdate[0] );
    }

    @Test
    public void snapshotFilesShouldReturnIndexFile() throws Exception
    {
        // when
        ResourceIterator<File> files = accessor.snapshotFiles();

        // then
        assertTrue( files.hasNext() );
        assertEquals( indexFile, files.next() );
        assertFalse( files.hasNext() );
    }

    @Test
    public void shouldSampleIndex() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
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

        try
        {
            // when
            accessor.newReader();
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void writingAfterDropShouldThrow() throws Exception
    {
        // given
        accessor.drop();

        try
        {
            // when
            accessor.newUpdater( IndexUpdateMode.ONLINE );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void readingAfterCloseShouldThrow() throws Exception
    {
        // given
        accessor.close();

        try
        {
            // when
            accessor.newReader();
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void writingAfterCloseShouldThrow() throws Exception
    {
        // given
        accessor.close();

        try
        {
            // when
            accessor.newUpdater( IndexUpdateMode.ONLINE );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    @Test
    public void shouldSeeAllEntriesInAllEntriesReader() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
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
    public void shouldSeeNoEntriesInAllEntriesReaderOnEmptyIndex() throws Exception
    {
        // when
        Set<Long> ids = asUniqueSet( accessor.newAllEntriesReader() );

        // then
        Set<Long> expectedIds = Collections.emptySet();
        assertEquals( expectedIds, ids );
    }

    private static Predicate<Value> lessThan( Double other )
    {
        return t -> compare( t, other ) < 0;
    }

    private static Predicate<Value> greaterThan( Double other )
    {
        return t -> compare( t, other ) > 0;
    }

    private void assertEntityIdHits( long[] expected, PrimitiveLongIterator result )
    {
        long[] actual = PrimitiveLongCollections.asArray( result );
        Arrays.sort( actual );
        Arrays.sort( expected );
        assertArrayEquals( expected, actual );
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
        return expectedData.toArray( new IndexEntryUpdate[expectedData.size()] )[random.nextInt( expectedData.size() )];
    }

    @SafeVarargs
    private final void processAll( IndexEntryUpdate<IndexDescriptor>... updates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
        {
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
        }
    }

    private void forceAndCloseAccessor() throws IOException
    {
        accessor.force();
        closeAccessor();
    }

    private void processAll( IndexUpdater updater, IndexEntryUpdate<IndexDescriptor>[] updates )
            throws IOException, IndexEntryConflictException
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

    // TODO: multiple query predicates... actually Lucene SimpleIndexReader only supports single predicate
    //       so perhaps we should wait with this until we know exactly how this works and which combinations
    //       that should be supported/optimized for.
}
