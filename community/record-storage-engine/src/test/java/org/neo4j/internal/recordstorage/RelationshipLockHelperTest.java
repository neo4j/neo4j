/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.bag.primitive.MutableLongBag;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.set.sorted.MutableSortedSet;
import org.eclipse.collections.impl.factory.primitive.LongBags;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.internal.recordstorage.RelationshipLockHelper.SortedSeekableList;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.neo4j.internal.recordstorage.TrackingResourceLocker.LockAcquisitionMonitor.NO_MONITOR;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.Record.isNull;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.api.txstate.RelationshipModifications.idsAsBatch;

@ExtendWith( RandomExtension.class )
class RelationshipLockHelperTest
{
    @Inject
    RandomRule random;

    @Test
    void shouldKeepListSorted()
    {
        MutableLongList shuffle = LongLists.mutable.empty();
        SortedSeekableList sortedListIterator = new SortedSeekableList( 0 );
        for ( int i = 0; i < 1000; i++ )
        {
            int value = random.nextInt( 1000 );
            sortedListIterator.add( value );
            shuffle.add( value );
            assertThat( sortedListIterator.underlyingList().toArray() ).isSorted();
        }

        shuffle.shuffleThis();
        shuffle.forEach( value -> {
            sortedListIterator.remove( value );
            assertThat( sortedListIterator.underlyingList().toArray() ).isSorted();
        } );

        assertThat( sortedListIterator.underlyingList().toArray() ).isEmpty();
    }

    @Test
    void canIterateWhileAddingAndRemoving()
    {
        MutableLongList shuffle = LongLists.mutable.empty();
        SortedSeekableList sortedListIterator = new SortedSeekableList( 0 );
        int maxValue = 1000;
        for ( int i = 0; i < 1000; i++ )
        {
            int value = random.nextInt( maxValue );
            sortedListIterator.add( value );
            shuffle.add( value );
            assertThat( sortedListIterator.underlyingList().toArray() ).isSorted();
        }

        while ( sortedListIterator.next() )
        {
            long value = sortedListIterator.get();
            long toAdd = random.nextInt( maxValue );
            sortedListIterator.add( toAdd );

            assertThat( value ).isEqualTo( value );

            if ( shuffle.notEmpty() )
            {
                long toRemove = shuffle.removeAtIndex( shuffle.size() - 1 );
                if ( toRemove != value )
                {
                    sortedListIterator.remove( toRemove );
                }
                assertThat( value ).isEqualTo( value );
            }

            if ( random.nextInt( 10 ) == 0 )
            {
                int backoff = random.nextInt( 5 );
                while ( sortedListIterator.prev() && backoff-- > 0 )
                {
                    //do nothing
                }
            }
        }
    }

    @Test
    void canTraverseUniques()
    {
        //Given
        SortedSeekableList sortedListIterator = new SortedSeekableList( 0 );
        MutableSortedSet<Long> uniques = SortedSets.mutable.of();
        for ( long i = 0; i < 100; i++ )
        {
            for ( int j = 0; j < random.nextInt( 5 ); j++ )
            {
                sortedListIterator.add( i );
                uniques.add( i );
            }
        }

        //Then
        uniques.forEach( unique ->
        {
            assertThat( sortedListIterator.nextUnique() ).isTrue();
            assertThat( sortedListIterator.get() ).isEqualTo( unique );
        } );

        assertThat( sortedListIterator.nextUnique() ).isFalse();

        //Then
        SortedSets.immutable.ofAll( Collections.reverseOrder(), uniques).forEach( unique ->
        {
            assertThat( sortedListIterator.prevUnique() ).isTrue();
            assertThat( sortedListIterator.get() ).isEqualTo( unique );
        } );
    }

    @ParameterizedTest
    @ValueSource( ints = {1, 10, 100, 1000} )
    void shouldTakeAllRelevantLocksForDeletion( int numNodes )
    {
        //Given
        MutableLongObjectMap<RecordAccess.RecordProxy<RelationshipRecord,Void>> proxies = LongObjectMaps.mutable.empty();
        MutableLongBag expectedLocks = LongBags.mutable.empty();
        MutableLongSet idsToDelete = LongSets.mutable.empty();
        int maxId = numNodes * 10;
        for ( int i = 0; i < numNodes; i++ )
        {
            long id = random.nextInt( maxId );
            if ( idsToDelete.add( id ) )
            {
                VolatileRelationshipRecord record = new VolatileRelationshipRecord( id, expectedLocks, maxId );
                var proxy = Mockito.mock( RecordAccess.RecordProxy.class );
                Mockito.when( proxy.forReadingLinkage() ).thenAnswer( invocation -> new RelationshipRecord( record.maybeChange() ) );
                proxies.put( id, proxy );
            }
        }
        RecordAccess<RelationshipRecord, Void> relRecords = Mockito.mock( RecordAccess.class );
        Mockito.when( relRecords.getOrLoad( Mockito.anyLong(), Mockito.any(), Mockito.any() ) )
                .thenAnswer( invocation -> proxies.get( invocation.getArgument( 0 ) ) );
        TrackingResourceLocker locks = new TrackingResourceLocker( random, NO_MONITOR ).withStrictAssertionsOn( ResourceTypes.RELATIONSHIP );

        //When
        RelationshipLockHelper.lockRelationshipsInOrder( idsAsBatch( idsToDelete ), NULL_REFERENCE.longValue(), relRecords, locks, PageCursorTracer.NULL,
                EmptyMemoryTracker.INSTANCE );

        //Then
        assertThat( locks.getExclusiveLocks( ResourceTypes.RELATIONSHIP ).toSortedArray() ).containsExactly( expectedLocks.toSet().toSortedArray() );
    }

    @ParameterizedTest
    @ValueSource( ints = {1, 10, 100, 1000} )
    void shouldFindAndLockEntrypoint( int chainLength )
    {
        //Given
        long nodeId = 42;
        List<RelationshipRecord> chain = createRelationshipChain( nodeId, chainLength );
        MutableLongObjectMap<RecordAccess.RecordProxy<RelationshipRecord,Void>> proxies = LongObjectMaps.mutable.empty();
        chain.forEach( record ->
        {
            RecordAccess.RecordProxy proxy = Mockito.mock( RecordAccess.RecordProxy.class );
            Mockito.when( proxy.getKey() ).thenAnswer( invocation -> record.getId() );
            Mockito.when( proxy.forReadingLinkage() ).thenAnswer( invocation -> record );
            Mockito.when( proxy.forReadingData() ).thenAnswer( invocation -> record );
            proxies.put( record.getId(), proxy );
        } );

        RecordAccess<RelationshipRecord, Void> relRecords = Mockito.mock( RecordAccess.class );
        Mockito.when( relRecords.getOrLoad( Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any() ) )
                .thenAnswer( invocation -> proxies.get( invocation.getArgument( 0 ) ) );

        double chanceOfGettingLock = Math.sqrt( 0.8 / ((double) chainLength * 0.5) );
        TrackingResourceLocker locks = new TrackingResourceLocker( random, NO_MONITOR, (int) (chanceOfGettingLock * 100.0) );

        //When
        RecordAccess.RecordProxy<RelationshipRecord,Void> entrypoint =
                RelationshipLockHelper.findAndLockEntrypoint( chain.get( 0 ).getId(), nodeId, relRecords, locks, LockTracer.NONE, PageCursorTracer.NULL );

        //Then
        long[] actualLocks = locks.getExclusiveLocks( ResourceTypes.RELATIONSHIP ).toArray();
        int expectedSize = 1;
        assertThat( entrypoint ).isNotNull();
        assertThat( actualLocks ).contains( entrypoint.getKey() );
        long next = entrypoint.forReadingLinkage().getNextRel( nodeId );
        if ( !isNull( next ) )
        {
            assertThat( actualLocks ).contains( next );
            expectedSize++;
        }
        assertThat( actualLocks ).hasSize( expectedSize );
    }

    private List<RelationshipRecord> createRelationshipChain( long nodeId, int chainLength )
    {
        List<RelationshipRecord> chain = new ArrayList<>();
        MutableLongSet usedIds = LongSets.mutable.empty();
        for ( int i = 0; i < chainLength; i++ )
        {
            long id;
            do
            {
                id = random.nextLong( 1000 );
            } while ( !usedIds.add( id ) );

            RelationshipRecord record = new RelationshipRecord( id );
            boolean firstInChain = i == 0;
            boolean inUse = true;
            boolean firstChain = random.nextBoolean();
            record.initialize( inUse, -1, firstChain ? nodeId : -1, !firstChain ? nodeId : -1, -1, -1, -1, -1, -1, firstChain && firstInChain,
                    !firstChain && firstInChain );
            if ( !firstInChain )
            {
                RelationshipRecord prev = chain.get( i - 1 );
                prev.setNextRel( id, nodeId );
                record.setPrevRel( prev.getId(), nodeId );
            }

            chain.add( record );
        }
        return chain;
    }

    private class VolatileRelationshipRecord extends RelationshipRecord
    {
        private final MutableLongBag usedIds;
        private final long maxId;
        private final boolean init;

        VolatileRelationshipRecord( long id, MutableLongBag usedIds, long maxId )
        {
            super( id );
            initialize( true, -1, -1, -1, -1, -1, -1, -1, -1, false, false );

            this.usedIds = usedIds;
            usedIds.add( id );
            this.maxId = maxId;
            maybeChange();
            init = true;
        }

        RelationshipRecord maybeChange()
        {
            if ( test() )
            {
                changeId( this::setFirstNextRel, this::getFirstNextRel );
            }
            if ( test() )
            {
                changeId( this::setFirstPrevRel, this::getFirstPrevRel );
            }
            if ( test() )
            {
                changeId( this::setSecondNextRel, this::getSecondNextRel );
            }
            if ( test() )
            {
                changeId( this::setSecondPrevRel, this::getSecondPrevRel );
            }
            return this;
        }

        private void changeId( Consumer<Long> consumer, Supplier<Long> supplier )
        {
            long oldId = supplier.get();
            usedIds.remove( oldId );
            long newId = randId( init );
            if ( newId != NULL_REFERENCE.longValue() )
            {
                usedIds.add( newId );
            }
            consumer.accept( newId );
        }

        private boolean test()
        {
            return !init || random.nextInt( (int) (maxId / 10) ) == 0;
        }

        private long randId( boolean allowNull )
        {
            if ( allowNull && test() )
            {
                return NULL_REFERENCE.longValue();
            }
            return random.nextLong( maxId );
        }
    }
}
