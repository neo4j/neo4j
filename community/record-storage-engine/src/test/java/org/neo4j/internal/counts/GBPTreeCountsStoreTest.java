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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.OtherThreadExecutor;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.test.OtherThreadExecutor.command;
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
    private FileSystemAbstraction fs;

    @Inject
    private RandomRule random;

    private GBPTreeCountsStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception
    {
        openCountsStore( CountsBuilder.EMPTY );
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
        long txId = BASE_TX_ID;
        try ( CountsAccessor.Updater updater = countsStore.apply( ++txId ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 10 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7 );
        }
        try ( CountsAccessor.Updater updater = countsStore.apply( ++txId ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 5 ); // now at 15
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 2 ); // now at 5
        }

        countsStore.checkpoint( UNLIMITED );

        // when/then
        assertEquals( 15, countsStore.nodeCount( LABEL_ID_1 ) );
        assertEquals( 5, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2 ) );
        assertEquals( 7, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2 ) );

        // and when
        try ( CountsAccessor.Updater updater = countsStore.apply( ++txId ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, -7 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, -5 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, -2 );
        }

        // then
        assertEquals( 8, countsStore.nodeCount( LABEL_ID_1 ) );
        assertEquals( 0, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2 ) );
        assertEquals( 5, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2 ) );
    }

    @Test
    void shouldCheckpointAndRecoverConsistentlyUnderStressfulLoad() throws Throwable
    {
        // given
        int threads = 50; // it's good if it's way more than number of cores so that it creates some scheduling issues
        int numberOfRounds = 5;
        int roundTimeMillis = 300;
        ConcurrentMap<CountsKey,AtomicLong> expected = new ConcurrentHashMap<>();
        AtomicLong nextTxId = new AtomicLong( BASE_TX_ID );
        AtomicLong lastCheckPointedTxId = new AtomicLong( nextTxId.longValue() );
        long lastRoundClosedAt = BASE_TX_ID;

        // Start at some number > 0 so that we can do negative deltas now and then
        long baseCount = 10_000;
        try ( CountsAccessor.Updater initialApplier = countsStore.apply( nextTxId.incrementAndGet() ) )
        {
            for ( int s = -1; s < HIGH_TOKEN_ID; s++ )
            {
                initialApplier.incrementNodeCount( s, baseCount );
                for ( int t = -1; t < HIGH_TOKEN_ID; t++ )
                {
                    for ( int e = -1; e < HIGH_TOKEN_ID; e++ )
                    {
                        initialApplier.incrementRelationshipCount( s, t, e, baseCount );
                    }
                }
            }
        }
        OutOfOrderSequence lastClosedTxId = new ArrayQueueOutOfOrderSequence( nextTxId.get(), 200, EMPTY_LONG_ARRAY );

        // when
        for ( int r = 0; r < numberOfRounds; r++ )
        {
            // Let loose updaters and a check-pointer
            Race race = new Race().withMaxDuration( roundTimeMillis, TimeUnit.MILLISECONDS );
            race.addContestants( threads, throwing( () ->
            {
                long txId = nextTxId.incrementAndGet();
                // Sleep a random time after getting the txId, this creates bigger temporary gaps in the txId sequence
                Thread.sleep( ThreadLocalRandom.current().nextInt( 5 ) );
                generateAndApplyTransaction( expected, txId );
                lastClosedTxId.offer( txId, EMPTY_LONG_ARRAY );
            } ) );
            race.addContestant( throwing( () ->
            {
                long checkpointTxId = lastClosedTxId.getHighestGapFreeNumber();
                countsStore.checkpoint( UNLIMITED );
                lastCheckPointedTxId.set( checkpointTxId );
                Thread.sleep( ThreadLocalRandom.current().nextInt( roundTimeMillis / 5 ) );
            } ) );
            race.go();

            // Crash here, well not really crash but close the counts store knowing that there's any number of transactions since the last checkpoint
            // and we know the last committed tx id as well as the (pessimistic) last check-pointed tx id.
            crashAndRestartCountsStore();
            recover( lastCheckPointedTxId.get(), nextTxId.get() );
            assertThat( nextTxId.get() ).isGreaterThan( lastRoundClosedAt );
            lastRoundClosedAt = nextTxId.get();

            // then
            assertCountsMatchesExpected( expected, baseCount );
        }
    }

    @Test
    void shouldNotReapplyAlreadyAppliedTransactionBelowHighestGapFree() throws Exception
    {
        // given
        int labelId = 5;
        long expectedCount = 0;
        int delta = 3;
        for ( long txId = BASE_TX_ID + 1; txId < 10; txId++ )
        {
            incrementNodeCount( txId, labelId, delta );
            expectedCount += delta;
        }
        assertEquals( expectedCount, countsStore.nodeCount( labelId ) );

        // when reapplying after a restart
        checkpointAndRestartCountsStore();

        for ( long txId = BASE_TX_ID + 1; txId < 10; txId++ )
        {
            incrementNodeCount( txId, labelId, delta );
        }
        // then it should not change the delta
        assertEquals( expectedCount, countsStore.nodeCount( labelId ) );
    }

    @Test
    void shouldNotReapplyAlreadyAppliedTransactionAmongStrayTxIds() throws Exception
    {
        // given
        int labelId = 20;
        incrementNodeCount( BASE_TX_ID + 1, labelId, 5 );
        // intentionally skip BASE_TX_ID + 2
        incrementNodeCount( BASE_TX_ID + 3, labelId, 7 );

        // when
        checkpointAndRestartCountsStore();
        incrementNodeCount( BASE_TX_ID + 3, labelId, 7 );
        assertEquals( 5 + 7, countsStore.nodeCount( labelId ) );
        incrementNodeCount( BASE_TX_ID + 2, labelId, 3 );

        // then
        assertEquals( 5 + 7 + 3, countsStore.nodeCount( labelId ) );
    }

    @Test
    void shouldUseCountsBuilderOnCreation() throws Exception
    {
        // given
        long rebuiltAtTransactionId = 5;
        int labelId = 3;
        int labelId2 = 6;
        int relationshipTypeId = 7;
        closeCountsStore();
        deleteCountsStore();

        // when
        TestableCountsBuilder builder = new TestableCountsBuilder( rebuiltAtTransactionId )
        {
            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                super.initialize( updater );
                updater.incrementNodeCount( labelId, 10 );
                updater.incrementRelationshipCount( labelId, relationshipTypeId, labelId2, 14 );
            }
        };
        openCountsStore( builder );
        assertTrue( builder.lastCommittedTxIdCalled );
        assertTrue( builder.initializeCalled );
        assertEquals( 10, countsStore.nodeCount( labelId ) );
        assertEquals( 0, countsStore.nodeCount( labelId2 ) );
        assertEquals( 14, countsStore.relationshipCount( labelId, relationshipTypeId, labelId2 ) );

        // and when
        checkpointAndRestartCountsStore();
        // Re-applying a txId below or equal to the "rebuild transaction id" should not apply it
        incrementNodeCount( rebuiltAtTransactionId - 1, labelId, 100 );
        assertEquals( 10, countsStore.nodeCount( labelId ) );
        incrementNodeCount( rebuiltAtTransactionId, labelId, 100 );
        assertEquals( 10, countsStore.nodeCount( labelId ) );

        // then
        incrementNodeCount( rebuiltAtTransactionId + 1, labelId, 100 );
        assertEquals( 110, countsStore.nodeCount( labelId ) );
    }

    @Test
    void shouldNotApplyTransactionOnCreatedCountsStoreDuringRecovery() throws IOException
    {
        // given
        int labelId = 123;
        incrementNodeCount( BASE_TX_ID + 1, labelId, 4 );
        countsStore.checkpoint( UNLIMITED );
        incrementNodeCount( BASE_TX_ID + 2, labelId, -2 );
        closeCountsStore();
        deleteCountsStore();
        GBPTreeCountsStore.Monitor monitor = mock( GBPTreeCountsStore.Monitor.class );
        // instantiate, but don't start
        instantiateCountsStore( new CountsBuilder()
        {
            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                updater.incrementNodeCount( labelId, 2 );
            }

            @Override
            public long lastCommittedTxId()
            {
                return BASE_TX_ID + 2;
            }
        }, false, monitor );

        // when doing recovery of the last transaction (since this is on an empty counts store then making the count negative, i.e. 0 - 2)
        // applying this negative delta would have failed in the updater.
        incrementNodeCount( BASE_TX_ID + 2, labelId, -2 );
        verify( monitor ).ignoredTransaction( BASE_TX_ID + 2 );
        countsStore.start();

        // then
        assertEquals( 2, countsStore.nodeCount( labelId ) );
    }

    @Test
    void checkpointShouldWaitForApplyingTransactionsToClose() throws Exception
    {
        // given
        CountsAccessor.Updater updater1 = countsStore.apply( BASE_TX_ID + 1 );
        CountsAccessor.Updater updater2 = countsStore.apply( BASE_TX_ID + 2 );

        try ( OtherThreadExecutor<Void> checkpointer = new OtherThreadExecutor<>( "Checkpointer", null ) )
        {
            // when
            Future<Object> checkpoint = checkpointer.executeDontWait( command( () -> countsStore.checkpoint( UNLIMITED ) ) );
            checkpointer.waitUntilWaiting();

            // and when closing one of the updaters it should still wait
            updater1.close();
            checkpointer.waitUntilWaiting();
            assertFalse( checkpoint.isDone() );

            // then when closing the other one it should be able to complete
            updater2.close();
            checkpoint.get();
        }
    }

    @Test
    void checkpointShouldBlockApplyingNewTransactions() throws Exception
    {
        // given
        CountsAccessor.Updater updaterBeforeCheckpoint = countsStore.apply( BASE_TX_ID + 1 );

        try ( OtherThreadExecutor<Void> checkpointer = new OtherThreadExecutor<>( "Checkpointer", null );
              OtherThreadExecutor<AtomicReference<CountsStore.Updater>> applier = new OtherThreadExecutor<>( "Applier", new AtomicReference<>() ) )
        {
            // when
            Future<Object> checkpoint = checkpointer.executeDontWait( command( () -> countsStore.checkpoint( UNLIMITED ) ) );
            checkpointer.waitUntilWaiting();

            // and when trying to open another applier it must wait
            Future<Void> applierAfterCheckpoint = applier.executeDontWait( state ->
            {
                state.set( countsStore.apply( BASE_TX_ID + 2 ) );
                return null;
            } );
            applier.waitUntilWaiting();
            assertFalse( checkpoint.isDone() );
            assertFalse( applierAfterCheckpoint.isDone() );

            // then when closing first updater the checkpoint should be able to complete
            updaterBeforeCheckpoint.close();
            checkpoint.get();

            // and then also the applier after the checkpoint should be able to continue
            applierAfterCheckpoint.get();
            applier.execute( state ->
            {
                state.get().close();
                return null;
            } );
        }
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly()
    {
        final File file = directory.file( "non-existing" );
        final IllegalStateException e = assertThrows( IllegalStateException.class,
                () -> new GBPTreeCountsStore( pageCache, file, immediate(), CountsBuilder.EMPTY, true, NO_MONITOR ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof NoSuchFileException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void shouldFailApplyInReadOnlyMode() throws IOException
    {
        // given
        countsStore.checkpoint( UNLIMITED );
        closeCountsStore();
        instantiateCountsStore( CountsBuilder.EMPTY, true, NO_MONITOR );
        countsStore.start();

        // then
        assertThrows( IllegalStateException.class, () -> countsStore.apply( BASE_TX_ID + 1 ) );
    }

    @Test
    void shouldNotCheckpointInReadOnlyMode() throws IOException
    {
        // given
        countsStore.checkpoint( UNLIMITED );
        closeCountsStore();
        instantiateCountsStore( CountsBuilder.EMPTY, true, NO_MONITOR );
        countsStore.start();

        // then it's fine to call checkpoint, because no changes can actually be made on a read-only counts store anyway
        countsStore.checkpoint( UNLIMITED );
    }

    @Test
    void shouldDumpCountsStore() throws IOException
    {
        // given
        long txId = BASE_TX_ID + 1;
        try ( CountsAccessor.Updater updater = countsStore.apply( txId ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 10 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7 );
        }
        countsStore.checkpoint( UNLIMITED );
        closeCountsStore();

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream( 1024 );
        GBPTreeCountsStore.dump( pageCache, countsStoreFile(), new PrintStream( out ) );

        // then
        String dump = out.toString();
        assertThat( dump ).contains( nodeKey( LABEL_ID_1 ) + " = 10" );
        assertThat( dump ).contains( relationshipKey( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2 ) + " = 3" );
        assertThat( dump ).contains( relationshipKey( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2 ) + " = 7" );
        assertThat( dump ).contains( "Highest gap-free txId: " + txId );
    }

    @Test
    void shouldNotSeeOutdatedCountsOnCheckpoint() throws Throwable
    {
        // given
        try ( CountsAccessor.Updater updater = countsStore.apply( BASE_TX_ID + 1 ) )
        {
            updater.incrementNodeCount( LABEL_ID_1, 10 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3 );
            updater.incrementRelationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7 );
        }

        // when
        Race race = new Race();
        race.addContestant( throwing( () ->
        {
            countsStore.checkpoint( UNLIMITED );
        } ), 1 );
        race.addContestants( 10, throwing( () ->
        {
            assertEquals( 10, countsStore.nodeCount( LABEL_ID_1 ) );
            assertEquals( 3, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2 ) );
            assertEquals( 7, countsStore.relationshipCount( LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2 ) );
        } ), 1 );

        // then
        race.go();
    }

    @Test
    void shouldNotCreateFileOnDumpingNonExistentCountsStore()
    {
        // given
        File file = directory.file( "abcd" );

        // when
        assertThrows( NoSuchFileException.class, () -> GBPTreeCountsStore.dump( pageCache, file, System.out ) );

        // then
        assertFalse( fs.fileExists( file ) );
    }

    private void incrementNodeCount( long txId, int labelId, int delta )
    {
        try ( CountsAccessor.Updater updater = countsStore.apply( txId ) )
        {
            updater.incrementNodeCount( labelId, delta );
        }
    }

    private void assertCountsMatchesExpected( ConcurrentMap<CountsKey,AtomicLong> source, long baseCount )
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
                visitCount( nodeKey( labelId ), count );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                visitCount( relationshipKey( startLabelId, typeId, endLabelId ), count );
            }

            private void visitCount( CountsKey key, long count )
            {
                AtomicLong expectedCount = expected.remove( key );
                if ( expectedCount == null )
                {
                    assertEquals( baseCount, count );
                }
                else
                {
                    assertEquals( baseCount + expectedCount.get(), count, () -> format( "Counts store has wrong count for %s", key ) );
                }
            }
        } );
        assertTrue( expected.isEmpty(), expected::toString );
    }

    private void recover( long lastCheckPointedTxId, long lastCommittedTxId )
    {
        ConcurrentMap<CountsKey,AtomicLong> throwAwayMap = new ConcurrentHashMap<>();
        for ( long txId = lastCheckPointedTxId + 1; txId <= lastCommittedTxId; txId++ )
        {
            generateAndApplyTransaction( throwAwayMap, txId );
        }
    }

    /**
     * Generates a transaction, i.e. a counts change set. The data is random, but uses a seed which is the seed of the {@link RandomRule} in this test
     * as well as the the supplied txId. Calling this method in any given test multiple times with any specific txId will generate the same data.
     *
     * @param expected map of counts to update with the generated changes.
     * @param txId transaction id to generate transaction data for and ultimately apply to the counts store (and the expected map).
     */
    private void generateAndApplyTransaction( ConcurrentMap<CountsKey,AtomicLong> expected, long txId )
    {
        Random rng = new Random( random.seed() + txId );
        try ( CountsAccessor.Updater updater = countsStore.apply( txId ) )
        {
            int numberOfKeys = rng.nextInt( 10 );
            for ( int j = 0; j < numberOfKeys; j++ )
            {
                long delta = rng.nextInt( 11 ) - 1; // chance to get -1
                CountsKey expectedKey;
                if ( rng.nextBoolean() )
                {   // Node
                    int labelId = randomTokenId( rng );
                    updater.incrementNodeCount( labelId, delta );
                    expectedKey = nodeKey( labelId );
                }
                else
                {   // Relationship
                    int startLabelId = randomTokenId( rng );
                    int type = randomTokenId( rng );
                    int endLabelId = randomTokenId( rng );
                    updater.incrementRelationshipCount( startLabelId, type, endLabelId, delta );
                    expectedKey = relationshipKey( startLabelId, type, endLabelId );
                }
                expected.computeIfAbsent( expectedKey, k -> new AtomicLong() ).addAndGet( delta );
            }
        }
    }

    private int randomTokenId( Random rng )
    {
        // i.e. also include chance for -1 which is the "any" token
        return rng.nextInt( HIGH_TOKEN_ID + 1 ) - 1;
    }

    private void checkpointAndRestartCountsStore() throws Exception
    {
        countsStore.checkpoint( UNLIMITED );
        closeCountsStore();
        openCountsStore();
    }

    private void crashAndRestartCountsStore() throws Exception
    {
        closeCountsStore();
        openCountsStore();
    }

    private void deleteCountsStore()
    {
        directory.getFileSystem().deleteFile( countsStoreFile() );
    }

    private File countsStoreFile()
    {
        return directory.file( "counts.db" );
    }

    private void openCountsStore( CountsBuilder builder ) throws IOException
    {
        instantiateCountsStore( builder, false, NO_MONITOR );
        countsStore.start();
    }

    private void instantiateCountsStore( CountsBuilder builder, boolean readOnly, GBPTreeCountsStore.Monitor monitor ) throws IOException
    {
        countsStore = new GBPTreeCountsStore( pageCache, countsStoreFile(), immediate(), builder, readOnly, monitor );
    }

    private static class TestableCountsBuilder implements CountsBuilder
    {
        private final long rebuiltAtTransactionId;
        boolean lastCommittedTxIdCalled;
        boolean initializeCalled;

        TestableCountsBuilder( long rebuiltAtTransactionId )
        {
            this.rebuiltAtTransactionId = rebuiltAtTransactionId;
        }

        @Override
        public void initialize( CountsAccessor.Updater updater )
        {
            initializeCalled = true;
        }

        @Override
        public long lastCommittedTxId()
        {
            lastCommittedTxIdCalled = true;
            return rebuiltAtTransactionId;
        }
    }
}
