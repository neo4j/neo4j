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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;

/**
 * Tests for
 * <ul>
 *     <li>{@link NativeSchemaNumberIndexAccessor}</li>
 *     <li>{@link NativeSchemaNumberIndexUpdater}</li>
 *     <li>{@link NativeSchemaNumberIndexAccessor.NativeSchemaNumberIndexReader}</li>
 * </ul>
 */
public abstract class NativeSchemaNumberIndexAccessorTest<KEY extends NumberKey, VALUE extends NumberValue>
        extends SchemaNumberIndexTestUtil<KEY,VALUE>
{
    private static final IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( 42, 666 );
    private NativeSchemaNumberIndexAccessor<KEY,VALUE> accessor;

    @Before
    public void setupAccessor() throws IOException
    {
        accessor = new NativeSchemaNumberIndexAccessor<>( pageCache, indexFile, layout, IMMEDIATE );
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
    public void removeMustThrowAfterClose() throws Exception
    {
        // given
        IndexUpdater updater = accessor.newUpdater( ONLINE );
        updater.close();

        // when
        try
        {
            updater.remove( Primitive.longSet() );
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
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();
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
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();
        processAll( updates );

        for ( int i = 0; i < updates.length; i++ )
        {
            IndexEntryUpdate<IndexDescriptor> update = updates[i];
            Object newValue;
            switch ( i % 3 )
            {
            case 0:
                newValue = (long) i;
                break;
            case 1:
                newValue = (float) i;
                break;
            case 2:
                newValue = (double) i;
                break;
            default:
                throw new IllegalArgumentException();
            }
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
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();
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
        Iterator<IndexEntryUpdate<IndexDescriptor>> newDataGenerator = randomUniqueUpdateGenerator( 0 );

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
        IndexReader reader = accessor.newReader();

        // when
        long count = reader.countIndexedNodes( 123, 456 );

        // then
        assertEquals( 0, count );
    }

    @Test
    public void shouldReturnCountOneForExistingData() throws Exception
    {
        // given
        IndexEntryUpdate[] updates = someIndexEntryUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();
        for ( IndexEntryUpdate update : updates )
        {
            long count = reader.countIndexedNodes( update.getEntityId(), update.values() );

            // then
            assertEquals( 1, count );
        }

        // and when
        long count = reader.countIndexedNodes( 123, 456 );

        // then
        assertEquals( 0, count );
    }

    @Test
    public void shouldReturnCountZeroForMismatchingData() throws Exception
    {
        // given
        IndexEntryUpdate[] updates = someIndexEntryUpdates();
        processAll( updates );

        // when
        IndexReader reader = accessor.newReader();

        for ( IndexEntryUpdate update : updates )
        {
            long countWithMismatchingData = reader.countIndexedNodes( update.getEntityId() + 1, update.values() );
            long countWithNonExistentEntityId = reader.countIndexedNodes( NON_EXISTENT_ENTITY_ID, update.values() );
            long countWithNonExistentValue = reader.countIndexedNodes( update.getEntityId(), NON_EXISTENT_VALUE );

            // then
            assertEquals( 0, countWithMismatchingData );
            assertEquals( 0, countWithNonExistentEntityId );
            assertEquals( 0, countWithNonExistentValue );
        }
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
                addition = add( update.getEntityId(), update.values()[0] );
                break;
            case CHANGED:
                addition = add( update.getEntityId(), update.values()[0] );
                removal = add( update.getEntityId(), update.beforeValues()[0] );
                break;
            case REMOVED:
                removal = add( update.getEntityId(), update.values()[0] );
                break;
            default:
                throw new IllegalArgumentException( update.updateMode().name() );
            }

            if ( addition != null )
            {
                expectedData.add( addition );
            }
            if ( removal != null )
            {
                expectedData.remove( removal );
            }
        }
    }

    private IndexEntryUpdate<IndexDescriptor>[] generateRandomUpdates( Set<IndexEntryUpdate<IndexDescriptor>> expectedData,
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
            processAll( updater, updates );
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
        return IndexEntryUpdate.add( 0, indexDescriptor, 0 );
    }

    // READER

    // shouldReturnZeroCountForEmptyIndex
    // shouldReturnCountOneForExistingData
    // shouldReturnCountZeroForMismatchingData

    // TODO: shouldReturnAllEntriesForExistsPredicate
    // TODO: shouldReturnNoEntriesForExistsPredicateForEmptyIndex
    // TODO: shouldReturnMatchingEntriesForExactPredicate
    // TODO: shouldReturnNoEntriesForMismatchingExactPredicate
    // TODO: shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd
    // TODO: shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd
    // TODO: shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd
    // TODO: shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd
    // TODO: shouldReturnNoEntriesForRangePredicateOutsideAnyMatch

    // TODO: SAMPLER

//    long countIndexedNodes( long nodeId, Object... propertyValues )
//    IndexSampler createSampler()
//    PrimitiveLongIterator query( IndexQuery... predicates )

    // ACCESSOR
    // todo shouldHandleMultipleConsecutiveUpdaters
    // todo requestForSecondUpdaterMustThrow

//    void drop()
//    IndexUpdater newUpdater( IndexUpdateMode mode )
//    void flush()
//    void force()
//    void close()
//    IndexReader newReader()
//    BoundedIterable<Long> newAllEntriesReader()
//    ResourceIterator<File> snapshotFiles()
//    void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
}
