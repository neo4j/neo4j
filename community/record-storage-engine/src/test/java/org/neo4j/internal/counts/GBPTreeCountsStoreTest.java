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
package org.neo4j.internal.counts;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.test.Race.throwing;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class GBPTreeCountsStoreTest
{
    private static final int HIGH_TOKEN_ID = 30;
    private static final int LABEL_ID_1 = 1;
    private static final int LABEL_ID_2 = 2;
    private static final int RELATIONSHIP_TYPE_ID_1 = 1;
    private static final int RELATIONSHIP_TYPE_ID_2 = 2;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomRule random;

    private GBPTreeCountsStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception
    {
        countsStore = new GBPTreeCountsStore( pageCache, directory.file( "counts.db" ), immediate(), CountsBuilder.EMPTY, false );
        countsStore.start();
    }

    @AfterEach
    void closeCountsStore()
    {
        countsStore.close();
    }

    @Test
    void shouldUpdateAndReadSomeCounts() throws IOException
    {
        // given
        try ( CountsAccessor.Updater updater = countsStore.apply( BASE_TX_ID + 1 ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 10 );

            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7 );
        }
        try ( CountsAccessor.Updater updater = countsStore.apply( BASE_TX_ID + 2 ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 5 ); // now at 15
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 2 ); // now at 5
        }

        countsStore.checkpoint( IOLimiter.UNLIMITED );

        // when/then
        DoubleLongRegister register = Registers.newDoubleLongRegister();

        countsStore.nodeCount( LABEL_ID_1, register );
        assertEquals( 15, register.readSecond() );

        countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, register );
        assertEquals( 5, register.readSecond() );

        countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, register );
        assertEquals( 7, register.readSecond() );
    }

    @Test
    void shouldCheckpointAndRecoverConsistentlyUnderStressfulLoad() throws Throwable
    {
        // given
        int threads = 50; // it's good if it's way more than number of cores so that it creates some scheduling issues
        int numberOfRounds = 10;
        int roundTimeMillis = 1_000;
        ConcurrentMap<CountsKey,AtomicLong> expected = new ConcurrentHashMap<>();
        AtomicLong nextTxId = new AtomicLong( BASE_TX_ID );
        AtomicLong lastCheckPointedTxId = new AtomicLong( nextTxId.longValue() );
        OutOfOrderSequence lastClosedTxId = new ArrayQueueOutOfOrderSequence( BASE_TX_ID, 200, EMPTY_LONG_ARRAY );
        long lastRoundClosedAt = BASE_TX_ID;

        // when
        for ( int r = 0; r < numberOfRounds; r++ )
        {
            // Let loose updaters and a check-pointer
            Race race = new Race().withMaxDuration( roundTimeMillis, TimeUnit.MILLISECONDS );
            race.addContestants( threads, throwing( () ->
            {
                Thread.sleep( ThreadLocalRandom.current().nextInt( 30 ) );
                long txId = nextTxId.incrementAndGet();
                applyTransaction( expected, txId );
                lastClosedTxId.offer( txId, EMPTY_LONG_ARRAY );
            } ) );
            race.addContestant( throwing( () ->
            {
                Thread.sleep( ThreadLocalRandom.current().nextInt( roundTimeMillis / 4 ) );
                long checkpointTxId = lastClosedTxId.getHighestGapFreeNumber();
                countsStore.checkpoint( IOLimiter.UNLIMITED );
                lastCheckPointedTxId.set( checkpointTxId );
            } ) );
            race.go();

            // Crash here, well not really crash but close the counts store knowing that there's any number of transactions since the last checkpoint
            // and we know the last committed tx id as well as the (pessimistic) last check-pointed tx id.
            closeCountsStore();

            // Open it and recover
            openCountsStore();
            recover( lastCheckPointedTxId.get(), nextTxId.get() );
            assertThat( nextTxId.get(), greaterThan( lastRoundClosedAt ) );
            lastRoundClosedAt = nextTxId.get();

            // then
            assertCountsMatchesExpected( expected );
        }
    }

    private void assertCountsMatchesExpected( ConcurrentMap<CountsKey,AtomicLong> source )
    {
        ConcurrentMap<CountsKey,AtomicLong> expected = new ConcurrentHashMap<>();
        source.entrySet().stream()
                .filter( entry -> entry.getValue().get() != 0 )                        // counts store won't have entries w/ 0 count
                .forEach( entry -> expected.put( entry.getKey(), entry.getValue() ) ); // copy them over to the one we're going to verify
        countsStore.accept( new CountsVisitor()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                visitCount( new CountsKey().initializeNode( labelId ), count );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                visitCount( new CountsKey().initializeRelationship( startLabelId, typeId, endLabelId ), count );
            }

            private void visitCount( CountsKey key, long count )
            {
                AtomicLong expectedCount = expected.remove( key );
                assertNotNull( expectedCount, () -> format( "Counts store has unexpected count key %s count:%d", key, count ) );
                assertEquals( expectedCount.get(), count, () -> format( "Counts store has wrong count for %s", key ) );
            }
        } );
        assertTrue( expected.isEmpty(), expected::toString );
    }

    private void recover( long lastCheckPointedTxId, long lastCommittedTxId )
    {
        ConcurrentMap<CountsKey,AtomicLong> throwAwayMap = new ConcurrentHashMap<>();
        for ( long txId = lastCheckPointedTxId + 1; txId <= lastCommittedTxId; txId++ )
        {
            applyTransaction( throwAwayMap, txId );
        }
    }

    private void applyTransaction( ConcurrentMap<CountsKey,AtomicLong> expected, long txId )
    {
        Random rng = new Random( random.seed() + txId );
        try ( CountsAccessor.Updater updater = countsStore.apply( txId ) )
        {
            int numberOfKeys = rng.nextInt( 10 );
            for ( int j = 0; j < numberOfKeys; j++ )
            {
                long delta = rng.nextInt( 10 );
                CountsKey expectedKey;
                if ( rng.nextBoolean() )
                {   // Node
                    int labelId = rng.nextInt( HIGH_TOKEN_ID );
                    updater.incrementNodeCount( labelId, delta );
                    expectedKey = new CountsKey().initializeNode( labelId );
                }
                else
                {   // Relationship
                    int startLabelId = rng.nextInt( HIGH_TOKEN_ID );
                    int type = rng.nextInt( HIGH_TOKEN_ID );
                    int endLabelId = rng.nextInt( HIGH_TOKEN_ID );
                    updater.incrementRelationshipCount( startLabelId, type, endLabelId, delta );
                    expectedKey = new CountsKey().initializeRelationship( startLabelId, type, endLabelId );
                }
                expected.computeIfAbsent( expectedKey, k -> new AtomicLong() ).addAndGet( delta );
            }
        }
    }
}
