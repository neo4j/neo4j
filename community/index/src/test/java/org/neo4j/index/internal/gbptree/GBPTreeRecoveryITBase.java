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
package org.neo4j.index.internal.gbptree;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.pagecache.IOLimiter.unlimited;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class GBPTreeRecoveryITBase<KEY,VALUE>
{
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule(
            config().withPageSize( PAGE_SIZE ).withAccessChecks( true ) );
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    // Test config
    private int loadCountTransactions;
    private int minInsertCountPerBatch;
    private int maxInsertCountPerBatch;
    private int minRemoveCountPerBatch;
    private int maxRemoveCountPerBatch;

    private static final int PAGE_SIZE = 256;
    private final Action CHECKPOINT = new CheckpointAction();

    private TestLayout<KEY,VALUE> layout;

    /* Global variables for recoverFromAnything test */
    private boolean recoverFromAnythingInitialized;
    private int keyRange;

    @Before
    public void setUp()
    {
        this.layout = getLayout( random );
        loadCountTransactions = random.intBetween( 300, 1_000 );
        minInsertCountPerBatch = 30;
        maxInsertCountPerBatch = 200;
        minRemoveCountPerBatch = 5;
        maxRemoveCountPerBatch = 20;
    }

    protected abstract TestLayout<KEY,VALUE> getLayout( RandomRule random );

    @Test
    public void shouldRecoverFromCrashBeforeFirstCheckpoint() throws Exception
    {
        // GIVEN
        // a tree with only small amount of data that has not yet seen checkpoint from outside
        KEY key = key( 1L );
        VALUE value = value( 10L );
        File file = directory.file( "index" );
        {
            try ( PageCache pageCache = createPageCache();
                  GBPTree<KEY,VALUE> index = createIndex( pageCache, file );
                  Writer<KEY,VALUE> writer = index.writer() )
            {
                writer.put( key, value );
                pageCache.flushAndForce();
                // No checkpoint
            }
        }

        // WHEN
        try ( PageCache pageCache = createPageCache();
              GBPTree<KEY,VALUE> index = createIndex( pageCache, file ) )
        {
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                writer.put( key, value );
            }

            // THEN
            // we should end up with a consistent index
            index.consistencyCheck();

            // ... containing all the stuff load says
            try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor =
                          index.seek( key( Long.MIN_VALUE ), key( Long.MAX_VALUE ) ) )
            {
                assertTrue( cursor.next() );
                Hit<KEY,VALUE> hit = cursor.get();
                assertEqualsKey( key, hit.key() );
                assertEqualsValue( value, hit.value() );
            }
        }
    }

    @Test
    public void shouldRecoverFromAnythingReplayExactFromCheckpointHighKeyContention() throws Exception
    {
        initializeRecoveryFromAnythingTest( 100 );
        doShouldRecoverFromAnything( true );
    }

    @Test
    public void shouldRecoverFromAnythingReplayFromBeforeLastCheckpointHighKeyContention() throws Exception
    {
        initializeRecoveryFromAnythingTest( 100 );
        doShouldRecoverFromAnything( false );
    }

    @Test
    public void shouldRecoverFromAnythingReplayExactFromCheckpointLowKeyContention() throws Exception
    {
        initializeRecoveryFromAnythingTest( 1_000_000 );
        doShouldRecoverFromAnything( true );
    }

    @Test
    public void shouldRecoverFromAnythingReplayFromBeforeLastCheckpointLowKeyContention() throws Exception
    {
        initializeRecoveryFromAnythingTest( 1_000_000 );
        doShouldRecoverFromAnything( false );
    }

    private void initializeRecoveryFromAnythingTest( int keyRange )
    {
        recoverFromAnythingInitialized = true;
        this.keyRange = keyRange;
    }

    private void assertInitialized()
    {
        assertTrue( recoverFromAnythingInitialized );
    }

    private void doShouldRecoverFromAnything( boolean replayRecoveryExactlyFromCheckpoint ) throws Exception
    {
        assertInitialized();
        // GIVEN
        // a tree which has had random updates and checkpoints in it, load generated with specific seed
        File file = directory.file( "index" );
        List<Action> load = generateLoad();
        List<Action> shuffledLoad = randomCausalAwareShuffle( load );
        int lastCheckPointIndex = indexOfLastCheckpoint( load );

        {
            // _,_,_,_,_,_,_,c,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,c,_,_,_,_,_,_,_,_,_,_,_
            //                                                 ^             ^
            //                                                 |             |------------ crash flush index
            //                                                 |-------------------------- last checkpoint index
            //

            PageCache pageCache = createPageCache();
            GBPTree<KEY,VALUE> index = createIndex( pageCache, file );
            // Execute all actions up to and including last checkpoint ...
            execute( shuffledLoad.subList( 0, lastCheckPointIndex + 1 ), index );
            // ... a random amount of the remaining "unsafe" actions ...
            int numberOfRemainingActions = shuffledLoad.size() - lastCheckPointIndex - 1;
            int crashFlushIndex = lastCheckPointIndex + random.nextInt( numberOfRemainingActions ) + 1;
            execute( shuffledLoad.subList( lastCheckPointIndex + 1, crashFlushIndex ), index );
            // ... flush ...
            pageCache.flushAndForce();
            // ... execute the remaining actions
            execute( shuffledLoad.subList( crashFlushIndex, shuffledLoad.size() ), index );
            // ... and finally crash
            fs.snapshot( throwing( () ->
            {
                index.close();
                pageCache.close();
            } ) );
        }

        // WHEN doing recovery
        List<Action> recoveryActions;
        if ( replayRecoveryExactlyFromCheckpoint )
        {
            recoveryActions = recoveryActions( load, lastCheckPointIndex + 1 );
        }
        else
        {
            recoveryActions = recoveryActions( load, random.nextInt( lastCheckPointIndex + 1) );
        }

        // first crashing during recovery
        int numberOfCrashesDuringRecovery = random.intBetween( 0, 3 );
        for ( int i = 0; i < numberOfCrashesDuringRecovery; i++ )
        {
            try ( PageCache pageCache = createPageCache();
                  GBPTree<KEY,VALUE> index = createIndex( pageCache, file ) )
            {
                int numberOfActionsToRecoverBeforeCrashing = random.intBetween( 1, recoveryActions.size() );
                recover( recoveryActions.subList( 0, numberOfActionsToRecoverBeforeCrashing ), index );
                // ... crash
            }
        }

        // to finally apply all actions after last checkpoint and verify tree
        try ( PageCache pageCache = createPageCache();
                GBPTree<KEY,VALUE> index = createIndex( pageCache, file ) )
        {
            recover( recoveryActions, index );

            // THEN
            // we should end up with a consistent index containing all the stuff load says
            index.consistencyCheck();
            long[/*key,value,key,value...*/] aggregate = expectedSortedAggregatedDataFromGeneratedLoad( load );
            try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor =
                    index.seek( key( Long.MIN_VALUE ), key( Long.MAX_VALUE ) ) )
            {
                for ( int i = 0; i < aggregate.length; )
                {
                    assertTrue( cursor.next() );
                    Hit<KEY,VALUE> hit = cursor.get();
                    assertEqualsKey( key( aggregate[i++] ), hit.key() );
                    assertEqualsValue( value( aggregate[i++] ), hit.value() );
                }
                assertFalse( cursor.next() );
            }
        }
    }

    /**
     * Shuffle actions without breaking causal dependencies, i.e. without affecting the end result
     * of the data ending up in the tree. Checkpoints cannot move.
     *
     * On an integration level with neo4j, this is done because of the nature of how concurrent transactions
     * are applied in random order and recovery applies transactions in order of transaction id.
     */
    private List<Action> randomCausalAwareShuffle( List<Action> actions )
    {
        //noinspection unchecked
        Action[] arrayToShuffle = actions.toArray( (Action[]) Array.newInstance( Action.class, actions.size() ) );
        int size = arrayToShuffle.length;
        int numberOfActionsToShuffle = random.nextInt( size / 2 );

        for ( int i = 0; i < numberOfActionsToShuffle; i++ )
        {
            int actionIndexToMove = random.nextInt( size );
            int stride = random.nextBoolean() ? 1 : -1;
            int maxNumberOfSteps = random.nextInt( 10 ) + 1;

            for ( int steps = 0; steps < maxNumberOfSteps; steps++ )
            {
                Action actionToMove = arrayToShuffle[actionIndexToMove];
                int actionIndexToSwap = actionIndexToMove + stride;
                if ( actionIndexToSwap < 0 || actionIndexToSwap >= size )
                {
                    break;
                }
                Action actionToSwap = arrayToShuffle[actionIndexToSwap];

                if ( actionToMove.hasCausalDependencyWith( actionToSwap ) )
                {
                    break;
                }

                arrayToShuffle[actionIndexToMove] = actionToSwap;
                arrayToShuffle[actionIndexToSwap] = actionToMove;

                actionIndexToMove = actionIndexToSwap;
            }
        }
        return Arrays.asList( arrayToShuffle );
    }

    private List<Action> recoveryActions( List<Action> load, int fromIndex )
    {
        return load.subList( fromIndex, load.size() ).stream()
                .filter( action -> !action.isCheckpoint() )
                .collect( Collectors.toList() );
    }

    private void recover( List<Action> load, GBPTree<KEY,VALUE> index ) throws IOException
    {
        execute( load, index );
    }

    private void execute( List<Action> load, GBPTree<KEY,VALUE> index )
            throws IOException
    {
        for ( Action action : load )
        {
            action.execute( index );
        }
    }

    private long[] expectedSortedAggregatedDataFromGeneratedLoad( List<Action> load )
    {
        TreeMap<Long,Long> map = new TreeMap<>();
        for ( Action action : load )
        {
            long[] data = action.data();
            if ( data != null )
            {
                for ( int i = 0; i < data.length; )
                {
                    long key = data[i++];
                    long value = data[i++];
                    if ( action.type() == ActionType.INSERT )
                    {
                        map.put( key, value );
                    }
                    else if ( action.type() == ActionType.REMOVE )
                    {
                        map.remove( key );
                    }
                    else
                    {
                        throw new UnsupportedOperationException( action.toString() );
                    }
                }
            }
        }

        @SuppressWarnings( "unchecked" )
        Map.Entry<Long,Long>[] entries = map.entrySet().toArray( new Map.Entry[map.size()] );
        long[] result = new long[entries.length * 2];
        for ( int i = 0, c = 0; i < entries.length; i++ )
        {
            result[c++] = entries[i].getKey();
            result[c++] = entries[i].getValue();
        }
        return result;
    }

    private int indexOfLastCheckpoint( List<Action> actions )
    {
        int i = 0;
        int lastCheckpoint = -1;
        for ( Action action : actions )
        {
            if ( action.isCheckpoint() )
            {
                lastCheckpoint = i;
            }
            i++;
        }
        return lastCheckpoint;
    }

    private List<Action> generateLoad()
    {
        List<Action> actions = new LinkedList<>();
        boolean hasCheckPoint = false;
        for ( int i = 0; i < loadCountTransactions; i++ )
        {
            Action action = randomAction( true );
            actions.add( action );
            if ( action == CHECKPOINT )
            {
                hasCheckPoint = true;
            }
        }

        // Guarantee that there's at least one check point, i.e. if there's none then append one at the end
        if ( !hasCheckPoint )
        {
            actions.add( CHECKPOINT );
        }

        // Guarantee that there are at least some non-checkpoint actions after last checkpoint
        if ( actions.get( actions.size() - 1 ) == CHECKPOINT )
        {
            int additional = random.intBetween( 1, 10 );
            for ( int i = 0; i < additional; i++ )
            {
                actions.add( randomAction( false ) );
            }
        }
        return actions;
    }

    private Action randomAction( boolean allowCheckPoint )
    {
        float randomized = random.nextFloat();
        if ( randomized <= 0.7 )
        {
            // put
            long[] data = modificationData( minInsertCountPerBatch, maxInsertCountPerBatch );
            return new InsertAction( data );
        }
        else if ( randomized <= 0.95 || !allowCheckPoint )
        {
            // remove
            long[] data = modificationData( minRemoveCountPerBatch, maxRemoveCountPerBatch );
            return new RemoveAction( data );
        }
        else
        {
            return CHECKPOINT;
        }
    }

    private long[] modificationData( int min, int max )
    {
        int count = random.intBetween( min, max );
        long[] data = new long[count * 2];
        for ( int i = 0, c = 0; i < count; i++ )
        {
            data[c++] = random.intBetween( 0, keyRange ); // key
            data[c++] = random.intBetween( 0, keyRange ); // value
        }
        return data;
    }

    private GBPTree<KEY,VALUE> createIndex( PageCache pageCache, File file ) throws IOException
    {
        return new GBPTreeBuilder<>( pageCache, file, layout ).build();
    }

    private PageCache createPageCache()
    {
        return pageCacheRule.getPageCache( fs.get() );
    }

    enum ActionType
    {
        INSERT, REMOVE, CHECKPOINT
    }

    abstract class Action
    {
        long[] data;
        Set<Long> allKeys;

        Action( long[] data )
        {
            this.data = data;
            this.allKeys = keySet( data );
        }

        long[] data()
        {
            return data;
        }

        abstract void execute( GBPTree<KEY,VALUE> index ) throws IOException;

        abstract boolean isCheckpoint();

        abstract boolean hasCausalDependencyWith( Action other );

        private Set<Long> keySet( long[] data )
        {
            Set<Long> keys = new TreeSet<>();
            for ( int i = 0; i < data.length; i += 2 )
            {
                keys.add( data[i] );
            }
            return keys;
        }

        abstract ActionType type();
    }

    abstract class DataAction extends Action
    {
        DataAction( long[] data )
        {
            super( data );
        }

        @Override
        boolean isCheckpoint()
        {
            return false;
        }

        @Override
        public boolean hasCausalDependencyWith( Action other )
        {
            if ( other.isCheckpoint() )
            {
                return true;
            }

            Set<Long> intersection = new TreeSet<>( allKeys );
            intersection.retainAll( other.allKeys );

            return !intersection.isEmpty();
        }
    }

    class InsertAction extends DataAction
    {
        InsertAction( long[] data )
        {
            super( data );
        }

        @Override
        public void execute( GBPTree<KEY,VALUE> index ) throws IOException
        {
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < data.length; )
                {
                    writer.put( key( data[i++] ), value( data[i++] ) );
                }
            }
        }

        @Override
        ActionType type()
        {
            return ActionType.INSERT;
        }
    }

    class RemoveAction extends DataAction
    {
        RemoveAction( long[] data )
        {
            super( data );
        }

        @Override
        public void execute( GBPTree<KEY,VALUE> index ) throws IOException
        {
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < data.length; )
                {
                    KEY key = key( data[i++] );
                    i++; // value
                    writer.remove( key );
                }
            }
        }

        @Override
        ActionType type()
        {
            return ActionType.REMOVE;
        }
    }

    class CheckpointAction extends Action
    {
        CheckpointAction()
        {
            super( new long[0] );
        }

        @Override
        public void execute( GBPTree<KEY,VALUE> index ) throws IOException
        {
            index.checkpoint( unlimited() );
        }

        @Override
        boolean isCheckpoint()
        {
            return true;
        }

        @Override
        public boolean hasCausalDependencyWith( Action other )
        {
            return true;
        }

        @Override
        ActionType type()
        {
            return ActionType.CHECKPOINT;
        }
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private void assertEqualsKey( KEY expected, KEY actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compare( expected, actual ) == 0 );
    }

    private void assertEqualsValue( VALUE expected, VALUE actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compareValue( expected, actual ) == 0 );
    }

    private long keySeed( KEY key )
    {
        return layout.keySeed( key );
    }

    private long valueSeed( VALUE value )
    {
        return layout.valueSeed( value );
    }
}
