/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import org.neo4j.function.Factory;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.test.Race;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper.Monitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.ParallelSort.Comparator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;

import static java.lang.Math.toIntExact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.count;
import static org.neo4j.helpers.progress.ProgressListener.NONE;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;

@RunWith( Parameterized.class )
public class EncodingIdMapperTest
{
    @Parameters( name = "processors:{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[]{1} );
        data.add( new Object[]{2} );
        int bySystem = Runtime.getRuntime().availableProcessors() - 1;
        if ( bySystem > 2 )
        {
            data.add( new Object[]{bySystem} );
        }
        return data;
    }

    private final int processors;
    private final Groups groups = new Groups();

    public EncodingIdMapperTest( int processors )
    {
        this.processors = processors;
    }

    @Test
    public void shouldHandleGreatAmountsOfStuff()
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<Object> inputIdLookup = String::valueOf;
        int count = 300_000;

        // WHEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            idMapper.put( inputIdLookup.apply( nodeId ), nodeId, GLOBAL );
        }
        idMapper.prepare( inputIdLookup, mock( Collector.class ), NONE );

        // THEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            Object id = inputIdLookup.apply( nodeId );
            if ( idMapper.get( id, GLOBAL ) == ID_NOT_FOUND )
            {
                fail( "Couldn't find " + id + " even though I added it just previously" );
            }
        }
    }

    @Test
    public void shouldReturnExpectedValueForNotFound()
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        idMapper.prepare( null, mock( Collector.class ), NONE );

        // WHEN
        long id = idMapper.get( "123", GLOBAL );

        // THEN
        assertEquals( ID_NOT_FOUND, id );
    }

    @Test
    public void shouldReportyProgressForSortAndDetect()
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        ProgressListener progress = mock( ProgressListener.class );
        idMapper.prepare( null, mock( Collector.class ), progress );

        // WHEN
        long id = idMapper.get( "123", GLOBAL );

        // THEN
        assertEquals( ID_NOT_FOUND, id );
        verify( progress, times( 3 ) ).started( anyString() );
        verify( progress, times( 3 ) ).done();
    }

    @Test
    public void shouldEncodeShortStrings()
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );

        // WHEN
        mapper.put( "123", 0, GLOBAL );
        mapper.put( "456", 1, GLOBAL );
        mapper.prepare( null, mock( Collector.class ), NONE );

        // THEN
        assertEquals( 1L, mapper.get( "456", GLOBAL ) );
        assertEquals( 0L, mapper.get( "123", GLOBAL ) );
    }

    @Test
    public void shouldEncodeSmallSetOfRandomData()
    {
        // GIVEN
        int size = random.nextInt( 10_000 ) + 2;
        ValueType type = ValueType.values()[random.nextInt( ValueType.values().length )];
        IdMapper mapper = mapper( type.encoder(), type.radix(), NO_MONITOR );

        // WHEN
        ValueGenerator values = new ValueGenerator( type.data( random.random() ) );
        for ( int nodeId = 0; nodeId < size; nodeId++ )
        {
            mapper.put( values.apply( nodeId ), nodeId, GLOBAL );
        }
        mapper.prepare( values, mock( Collector.class ), NONE );

        // THEN
        for ( int nodeId = 0; nodeId < size; nodeId++ )
        {
            Object value = values.values.get( nodeId );
            assertEquals( "Expected " + value + " to map to " + nodeId, nodeId, mapper.get( value, GLOBAL ) );
        }
    }

    @Test
    public void shouldReportCollisionsForSameInputId()
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<Object> values = values( "10", "9", "10" );
        for ( int i = 0; i < 3; i++ )
        {
            mapper.put( values.apply( i ), i, GLOBAL );
        }

        // WHEN
        Collector collector = mock( Collector.class );
        mapper.prepare( values, collector, NONE );

        // THEN
        verify( collector, times( 1 ) ).collectDuplicateNode( "10", 2, GLOBAL.name() );
        verifyNoMoreInteractions( collector );
    }

    private static LongFunction<Object> wrap( List<Object> ids )
    {
        return nodeId -> ids.get( toIntExact( nodeId ) );
    }

    @Test
    public void shouldCopeWithCollisionsBasedOnDifferentInputIds()
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Encoder encoder = mock( Encoder.class );
        when( encoder.encode( any() ) ).thenReturn( 12345L );
        IdMapper mapper = mapper( encoder, Radix.STRING, monitor );
        LongFunction<Object> ids = values( "10", "9" );
        for ( int i = 0; i < 2; i++ )
        {
            mapper.put( ids.apply( i ), i, GLOBAL );
        }

        // WHEN
        ProgressListener progress = mock( ProgressListener.class );
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, progress );

        // THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 2 );
        assertEquals( 0L, mapper.get( "10", GLOBAL ) );
        assertEquals( 1L, mapper.get( "9", GLOBAL ) );
        // 7 times since SPLIT+SORT+DETECT+RESOLVE+SPLIT+SORT,DEDUPLICATE
        verify( progress, times( 7 ) ).started( anyString() );
        verify( progress, times( 7 ) ).done();
    }

    @Test
    public void shouldCopeWithMixedActualAndAccidentalCollisions()
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Encoder encoder = mock( Encoder.class );
        // Create these explicit instances so that we can use them in mock, even for same values
        String a = new String( "a" );
        String b = new String( "b" );
        String c = new String( "c" );
        String a2 = new String( "a" );
        String e = new String( "e" );
        String f = new String( "f" );
        when( encoder.encode( a ) ).thenReturn( 1L );
        when( encoder.encode( b ) ).thenReturn( 1L );
        when( encoder.encode( c ) ).thenReturn( 3L );
        when( encoder.encode( a2 ) ).thenReturn( 1L );
        when( encoder.encode( e ) ).thenReturn( 2L );
        when( encoder.encode( f ) ).thenReturn( 1L );
        Group groupA = groups.getOrCreate( "A" );
        Group groupB = groups.getOrCreate( "B" );
        IdMapper mapper = mapper( encoder, Radix.STRING, monitor );
        LongFunction<Object> ids = values( "a", "b", "c", "a", "e", "f" );
        Group[] groups = new Group[] {groupA, groupA, groupA, groupB, groupB, groupB};

        // a/A --> 1
        // b/A --> 1 accidental collision with a/A
        // c/A --> 3
        // a/B --> 1 actual collision with a/A
        // e/B --> 2
        // f/B --> 1 accidental collision with a/A

        // WHEN
        for ( int i = 0; i < 6; i++ )
        {
            mapper.put( ids.apply( i ), i, groups[i] );
        }
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, mock( ProgressListener.class ) );

        // THEN
        verify( monitor ).numberOfCollisions( 4 );
        assertEquals( 0L, mapper.get( a, groupA ) );
        assertEquals( 1L, mapper.get( b, groupA ) );
        assertEquals( 2L, mapper.get( c, groupA ) );
        assertEquals( 3L, mapper.get( a2, groupB ) );
        assertEquals( 4L, mapper.get( e, groupB ) );
        assertEquals( 5L, mapper.get( f, groupB ) );
    }

    @Test
    public void shouldBeAbleToHaveDuplicateInputIdButInDifferentGroups()
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Group firstGroup = groups.getOrCreate( "first" );
        Group secondGroup = groups.getOrCreate( "second" );
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, monitor );
        LongFunction<Object> ids = values( "10", "9", "10" );
        int id = 0;
        // group 0
        mapper.put( ids.apply( id ), id++, firstGroup );
        mapper.put( ids.apply( id ), id++, firstGroup );
        // group 1
        mapper.put( ids.apply( id ), id++, secondGroup );
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, NONE );

        // WHEN/THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 0 );
        assertEquals( 0L, mapper.get( "10", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", firstGroup ) );
        assertEquals( 2L, mapper.get( "10", secondGroup ) );
        assertFalse( mapper.leftOverDuplicateNodesIds().hasNext() );
    }

    @Test
    public void shouldOnlyFindInputIdsInSpecificGroup()
    {
        // GIVEN
        Group firstGroup = groups.getOrCreate( "first" );
        Group secondGroup = groups.getOrCreate( "second" );
        Group thirdGroup = groups.getOrCreate( "third" );
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<Object> ids = values( "8", "9", "10" );
        int id = 0;
        mapper.put( ids.apply( id ), id++, firstGroup );
        mapper.put( ids.apply( id ), id++, secondGroup );
        mapper.put( ids.apply( id ), id++, thirdGroup );
        mapper.prepare( ids, mock( Collector.class ), NONE );

        // WHEN/THEN
        assertEquals( 0L, mapper.get( "8", firstGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "8", secondGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "8", thirdGroup ) );

        assertEquals( ID_NOT_FOUND, mapper.get( "9", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", secondGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "9", thirdGroup ) );

        assertEquals( ID_NOT_FOUND, mapper.get( "10", firstGroup ) );
        assertEquals( ID_NOT_FOUND, mapper.get( "10", secondGroup ) );
        assertEquals( 2L, mapper.get( "10", thirdGroup ) );
    }

    @Test
    public void shouldHandleManyGroups()
    {
        // GIVEN
        int size = 256; // which results in GLOBAL (0) + 1-256 = 257 groups, i.e. requiring two bytes
        for ( int i = 0; i < size; i++ )
        {
            groups.getOrCreate( "" + i );
        }
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );

        // WHEN
        for ( int i = 0; i < size; i++ )
        {
            mapper.put( i, i, groups.get( "" + i ) );
        }
        // null since this test should have been set up to not run into collisions
        mapper.prepare( null, mock( Collector.class ), NONE );

        // THEN
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( i, mapper.get( i, groups.get( "" + i ) ) );
        }
    }

    @Test
    public void shouldDetectCorrectDuplicateInputIdsWhereManyAccidentalInManyGroups()
    {
        // GIVEN
        final ControlledEncoder encoder = new ControlledEncoder( new LongEncoder() );
        final int idsPerGroup = 20;
        int groupCount = 5;
        for ( int i = 0; i < groupCount; i++ )
        {
            groups.getOrCreate( "Group " + i );
        }
        IdMapper mapper = mapper( encoder, Radix.LONG, NO_MONITOR, ParallelSort.DEFAULT,
                numberOfCollisions -> new LongCollisionValues( NumberArrayFactory.HEAP, numberOfCollisions ) );
        final AtomicReference<Group> group = new AtomicReference<>();
        LongFunction<Object> ids = nodeId ->
        {
            int groupId = toIntExact( nodeId / idsPerGroup );
            if ( groupId == groupCount )
            {
                return null;
            }
            group.set( groups.get( groupId ) );

            // Let the first 10% in each group be accidental collisions with each other
            // i.e. all first 10% in each group collides with all other first 10% in each group
            if ( nodeId % idsPerGroup < 2 )
            {   // Let these colliding values encode into the same eId as well,
                // so that they are definitely marked as collisions
                encoder.useThisIdToEncodeNoMatterWhatComesIn( Long.valueOf( 1234567 ) );
                return Long.valueOf( nodeId % idsPerGroup );
            }

            // The other 90% will be accidental collisions for something else
            encoder.useThisIdToEncodeNoMatterWhatComesIn( Long.valueOf( 123456 - group.get().id() ) );
            return Long.valueOf( nodeId );
        };

        // WHEN
        int count = idsPerGroup * groupCount;
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            mapper.put( ids.apply( nodeId ), nodeId, group.get() );
        }
        Collector collector = mock( Collector.class );

        mapper.prepare( ids, collector, NONE );

        // THEN
        verifyNoMoreInteractions( collector );
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            assertEquals( nodeId, mapper.get( ids.apply( nodeId ), group.get() ) );
        }
        verifyNoMoreInteractions( collector );
        assertFalse( mapper.leftOverDuplicateNodesIds().hasNext() );
    }

    @Test
    public void shouldHandleHolesInIdSequence()
    {
        // GIVEN
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );
        List<Object> ids = new ArrayList<>();
        for ( int i = 0; i < 100; i++ )
        {
            if ( random.nextBoolean() )
            {
                // Skip this one
            }
            else
            {
                Long id = (long) i;
                ids.add( id );
                mapper.put( id, i, GLOBAL );
            }
        }

        // WHEN
        mapper.prepare( values( ids.toArray() ), mock( Collector.class ), NONE );

        // THEN
        for ( Object id : ids )
        {
            assertEquals( ((Long)id).longValue(), mapper.get( id, GLOBAL ) );
        }
    }

    @Test
    public void shouldHandleLargeAmountsOfDuplicateNodeIds()
    {
        // GIVEN
        IdMapper mapper = mapper( new LongEncoder(), Radix.LONG, NO_MONITOR );
        long nodeId = 0;
        int high = 10;
        // a list of input ids
        List<Object> ids = new ArrayList<>();
        for ( int run = 0; run < 2; run++ )
        {
            for ( long i = 0; i < high / 2; i++ )
            {
                ids.add( high - (i + 1) );
                ids.add( i );
            }
        }
        // fed to the IdMapper
        for ( Object inputId : ids )
        {
            mapper.put( inputId, nodeId++, GLOBAL );
        }

        // WHEN
        Collector collector = mock( Collector.class );
        mapper.prepare( values( ids.toArray() ), collector, NONE );

        // THEN
        verify( collector, times( high ) ).collectDuplicateNode( any( Object.class ), anyLong(), anyString() );
        assertEquals( high, count( mapper.leftOverDuplicateNodesIds() ) );
    }

    @Test
    public void shouldDetectLargeAmountsOfCollisions()
    {
        // GIVEN
        IdMapper mapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        int count = 20_000;
        List<Object> ids = new ArrayList<>();
        long id = 0;

        // Generate and add all input ids
        for ( int elements = 0; elements < count; elements++ )
        {
            String inputId = UUID.randomUUID().toString();
            for ( int i = 0; i < 2; i++ )
            {
                ids.add( inputId );
                mapper.put( inputId, id++, GLOBAL );
            }
        }

        // WHEN
        CountingCollector collector = new CountingCollector();
        mapper.prepare( values( ids.toArray() ), collector, NONE );

        // THEN
        assertEquals( count, collector.count );
    }

    @Test
    public void shouldPutFromMultipleThreads() throws Throwable
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        AtomicLong highNodeId = new AtomicLong();
        int batchSize = 1234;
        Race race = new Race();
        LongFunction<Object> inputIdLookup = String::valueOf;
        int countPerThread = 30_000;
        race.addContestants( processors, () ->
        {
            int cursor = batchSize;
            long nextNodeId = 0;
            for ( int j = 0; j < countPerThread; j++ )
            {
                if ( cursor == batchSize )
                {
                    nextNodeId = highNodeId.getAndAdd( batchSize );
                    cursor = 0;
                }
                long nodeId = nextNodeId++;
                cursor++;

                idMapper.put( inputIdLookup.apply( nodeId ), nodeId, GLOBAL );
            }
        } );

        // WHEN
        race.go();
        idMapper.prepare( inputIdLookup, mock( Collector.class ), ProgressListener.NONE );

        // THEN
        int count = processors * countPerThread;
        int countWithGapsWorstCase = count + batchSize * processors;
        int correctHits = 0;
        for ( long nodeId = 0; nodeId < countWithGapsWorstCase; nodeId++ )
        {
            long result = idMapper.get( inputIdLookup.apply( nodeId ), GLOBAL );
            if ( result != -1 )
            {
                assertEquals( nodeId, result );
                correctHits++;
            }
        }
        assertEquals( count, correctHits );
    }

    private LongFunction<Object> values( Object... values )
    {
        return value -> values[toIntExact( value )];
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, Monitor monitor )
    {
        return mapper( encoder, radix, monitor, ParallelSort.DEFAULT );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, Monitor monitor, Comparator comparator )
    {
        return mapper( encoder, radix, monitor, comparator, autoDetect( encoder ) );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, Monitor monitor, Comparator comparator,
            LongFunction<CollisionValues> collisionValuesFactory )
    {
        return new EncodingIdMapper( NumberArrayFactory.HEAP, encoder, radix, monitor, RANDOM_TRACKER_FACTORY, groups,
                collisionValuesFactory, 1_000, processors, comparator );
    }

    private LongFunction<CollisionValues> autoDetect( Encoder encoder )
    {
        return numberOfCollisions -> encoder instanceof LongEncoder
                ? new LongCollisionValues( NumberArrayFactory.HEAP, numberOfCollisions )
                : new StringCollisionValues( NumberArrayFactory.HEAP, numberOfCollisions );

    }

    private static final TrackerFactory RANDOM_TRACKER_FACTORY =
            ( arrayFactory, size ) -> System.currentTimeMillis() % 2 == 0
                    ? new IntTracker( arrayFactory.newIntArray( size, IntTracker.DEFAULT_VALUE ) )
                    : new BigIdTracker( arrayFactory.newByteArray( size, BigIdTracker.DEFAULT_VALUE ) );

    private class ValueGenerator implements LongFunction<Object>
    {
        private final Factory<Object> generator;
        private final List<Object> values = new ArrayList<>();
        private final Set<Object> deduper = new HashSet<>();

        ValueGenerator( Factory<Object> generator )
        {
            this.generator = generator;
        }

        @Override
        public Object apply( long nodeId )
        {
            while ( true )
            {
                Object value = generator.newInstance();
                if ( deduper.add( value ) )
                {
                    values.add( value );
                    return value;
                }
            }
        }
    }

    private enum ValueType
    {
        LONGS
        {
            @Override
            Encoder encoder()
            {
                return new LongEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.LONG;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return () -> random.nextInt( 1_000_000_000 );
            }
        },
        LONGS_AS_STRINGS
        {
            @Override
            Encoder encoder()
            {
                return new StringEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.STRING;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return () -> String.valueOf( random.nextInt( 1_000_000_000 ) );
            }
        },
        VERY_LONG_STRINGS
        {
            char[] CHARS = "½!\"#¤%&/()=?`´;:,._-<>".toCharArray();

            @Override
            Encoder encoder()
            {
                return new StringEncoder();
            }

            @Override
            Factory<Radix> radix()
            {
                return Radix.STRING;
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return new Factory<Object>()
                {
                    @Override
                    public Object newInstance()
                    {
                        // Randomize length, although reduce chance of really long strings
                        int length = 1500;
                        for ( int i = 0; i < 4; i++ )
                        {
                            length = random.nextInt( length ) + 20;
                        }
                        char[] chars = new char[length];
                        for ( int i = 0; i < length; i++ )
                        {
                            char ch;
                            if ( random.nextBoolean() )
                            {   // A letter
                                ch = randomLetter( random );
                            }
                            else
                            {
                                ch = CHARS[random.nextInt( CHARS.length )];
                            }
                            chars[i] = ch;
                        }
                        return new String( chars );
                    }

                    private char randomLetter( Random random )
                    {
                        int base;
                        if ( random.nextBoolean() )
                        {   // lower case
                            base = 'a';
                        }
                        else
                        {   // upper case
                            base = 'A';
                        }
                        int size = 'z' - 'a';
                        return (char) (base + random.nextInt( size ));
                    }
                };
            }
        };

        abstract Encoder encoder();

        abstract Factory<Radix> radix();

        abstract Factory<Object> data( Random random );
    }

    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    private static class CountingCollector implements Collector
    {
        private int count;

        @Override
        public void collectBadRelationship( Object startId, String startIdGroup, String type, Object endId,
                String endIdGroup, Object specificValue )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void collectDuplicateNode( Object id, long actualId, String group )
        {
            count++;
        }

        @Override
        public boolean isCollectingBadRelationships()
        {
            return false;
        }

        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long badEntries()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    }
}
