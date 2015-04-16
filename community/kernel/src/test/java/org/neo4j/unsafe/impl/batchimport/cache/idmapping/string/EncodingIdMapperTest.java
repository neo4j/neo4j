/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper.Monitor;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIterator;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.progress.ProgressListener.NONE;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;
import static org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper.wrap;

public class EncodingIdMapperTest
{
    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( NumberArrayFactory.AUTO );
        InputIterable<Object> ids = new InputIterable<Object>()
        {
            @Override
            public InputIterator<Object> iterator()
            {
                return new InputIterator.Adapter<Object>()
                {
                    private int i;

                    @Override
                    public boolean hasNext()
                    {
                        return i < 300_000;
                    }

                    @Override
                    public Object next()
                    {
                        return "" + (i++);
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return false;
            }
        };

        // WHEN
        long index = 0;
        for ( Object id : ids )
        {
            idMapper.put( id, index++, GLOBAL );
        }
        idMapper.prepare( ids, mock( Collector.class ), NONE );
        MemoryStatsVisitor memoryStats = new GatheringMemoryStatsVisitor();
        idMapper.acceptMemoryStatsVisitor( memoryStats );

        // THEN
        for ( Object id : ids )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            if ( idMapper.get( id, GLOBAL ) == -1 )
            {
                fail( "Couldn't find " + id + " even though I added it just previously" );
            }
        }
    }

    @Test
    public void shouldReturnExpectedValueForNotFound() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( NumberArrayFactory.AUTO );
        idMapper.prepare( null, mock( Collector.class ), NONE );

        // WHEN
        long id = idMapper.get( "123", GLOBAL );

        // THEN
        assertEquals( -1L, id );
    }

    @Test
    public void shouldReportyProgressForSortAndDetect() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( NumberArrayFactory.AUTO );
        ProgressListener progress = mock( ProgressListener.class );
        idMapper.prepare( null, mock( Collector.class ), progress );

        // WHEN
        long id = idMapper.get( "123", GLOBAL );

        // THEN
        assertEquals( -1L, id );
        verify( progress, times( 3 ) ).started( anyString() );
        verify( progress, times( 3 ) ).done();
    }

    @Test
    public void shouldEncodeShortStrings() throws Exception
    {
        // GIVEN
        IdMapper mapper = IdMappers.strings( NumberArrayFactory.AUTO );

        // WHEN
        mapper.put( "123", 0, GLOBAL );
        mapper.put( "456", 1, GLOBAL );
        mapper.prepare( null, mock( Collector.class ), NONE );

        // THEN
        assertEquals( 1L, mapper.get( "456", GLOBAL ) );
        assertEquals( 0L, mapper.get( "123", GLOBAL ) );
    }

    @Repeat( times = 10 )
    @Test
    public void shouldEncodeSmallSetOfRandomData() throws Throwable
    {
        // GIVEN
        int processorsForSorting = random.nextInt( 7 ) + 1;
        int size = random.nextInt( 10_000 ) + 2;
        ValueType type = ValueType.values()[random.nextInt( ValueType.values().length )];
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, type.encoder(), type.radix(),
                NO_MONITOR, size*2, processorsForSorting /*1-7*/ );

        // WHEN
        InputIterable<Object> values = new ValueGenerator( size, type.data( random ) );
        {
            int id = 0;
            for ( Object value : values )
            {
                mapper.put( value, id++, GLOBAL );
            }
        }

        try
        {
            mapper.prepare( values, mock( Collector.class ), NONE );

            // THEN
            int id = 0;
            for ( Object value : values )
            {
                assertEquals( "Expected " + value + " to map to " + id + ", seed:" + seed, id++,
                        mapper.get( value, GLOBAL ) );
            }
        }
        catch ( Throwable e )
        {
            throw Exceptions.withMessage( e, e.getMessage() + ", seed:" + seed );
        }
    }

    @Test
    public void shouldReportCollisionsForSameInputId() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String(),
                NO_MONITOR );
        InputIterable<Object> ids = wrap( "source", Arrays.<Object>asList( "10", "9", "10" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, GLOBAL );
            }
        }

        // WHEN
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, NONE );

        // THEN
        verify( collector, times( 1 ) ).collectDuplicateNode( "10", 2, GLOBAL.name(), "source:1", "source:3" );
        verifyNoMoreInteractions( collector );
    }

    @Test
    public void shouldIncludeSourceLocationsOfCollisions() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String(),
                NO_MONITOR );
        final List<Object> idList = Arrays.<Object>asList( "10", "9", "10" );
        InputIterable<Object> ids = wrap( "source", idList );

        Group group = new Group.Adapter( GLOBAL.id(), "global" );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, group );
            }
        }

        // WHEN
        try
        {
            mapper.prepare( ids, badCollector( new ByteArrayOutputStream(), 0 ), NONE );
            fail( "Should have failed" );
        }
        catch ( DuplicateInputIdException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "10" ) );
            assertThat( e.getMessage(), containsString( "source:1" ) );
            assertThat( e.getMessage(), containsString( "source:3" ) );
        }
    }

    @Test
    public void shouldCopeWithCollisionsBasedOnDifferentInputIds() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        Encoder encoder = mock( Encoder.class );
        when( encoder.encode( any() ) ).thenReturn( 12345L );
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, encoder, new Radix.String(), monitor );
        InputIterable<Object> ids = wrap( "source", Arrays.<Object>asList( "10", "9" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, GLOBAL );
            }
        }

        // WHEN
        ProgressListener progress = mock( ProgressListener.class );
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, progress );

        // THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 1 );
        assertEquals( 0L, mapper.get( "10", GLOBAL ) );
        assertEquals( 1L, mapper.get( "9", GLOBAL ) );
        // 3 times since SORT+DETECT+RESOLVE
        verify( progress, times( 4 ) ).started( anyString() );
        verify( progress, times( 4 ) ).done();
    }

    @Test
    public void shouldCopeWithMixedActualAndAccidentalCollisions() throws Exception
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
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, encoder, new Radix.String(), monitor );
        InputIterable<Object> ids = wrap( "source", Arrays.<Object>asList( "a", "b", "c", "a", "e", "f" ) );
        Group.Adapter groupA = new Group.Adapter( 1, "A" );
        Group.Adapter groupB = new Group.Adapter( 2, "B" );
        Group[] groups = new Group[] {groupA, groupA, groupA, groupB, groupB, groupB};

        // a/A --> 1
        // b/A --> 1 accidental collision with a/A
        // c/A --> 3
        // a/B --> 1 actual collision with a/A
        // e/B --> 2
        // f/B --> 1 accidental collision with a/A

        // WHEN
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, groups[i] );
            }
        }
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, mock( ProgressListener.class ) );

        // THEN
        verify( monitor ).numberOfCollisions( 2 );
        assertEquals( 0L, mapper.get( a, groupA ) );
        assertEquals( 1L, mapper.get( b, groupA ) );
        assertEquals( 2L, mapper.get( c, groupA ) );
        assertEquals( 3L, mapper.get( a2, groupB ) );
        assertEquals( 4L, mapper.get( e, groupB ) );
        assertEquals( 5L, mapper.get( f, groupB ) );
    }

    @Test
    public void shouldBeAbleToHaveDuplicateInputIdButInDifferentGroups() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String(),
                monitor );
        InputIterable<Object> ids = wrap( "source", Arrays.<Object>asList( "10", "9", "10" ) );
        Groups groups = new Groups();
        Group firstGroup = groups.getOrCreate( "first" ), secondGroup = groups.getOrCreate( "second" );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            int id = 0;
            // group 0
            mapper.put( iterator.next(), id++, firstGroup );
            mapper.put( iterator.next(), id++, firstGroup );
            // group 1
            mapper.put( iterator.next(), id++, secondGroup );
        }
        Collector collector = mock( Collector.class );
        mapper.prepare( ids, collector, NONE );

        // WHEN/THEN
        verifyNoMoreInteractions( collector );
        verify( monitor ).numberOfCollisions( 0 );
        assertEquals( 0L, mapper.get( "10", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", firstGroup ) );
        assertEquals( 2L, mapper.get( "10", secondGroup ) );
    }

    @Test
    public void shouldOnlyFindInputIdsInSpecificGroup() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String(),
                NO_MONITOR );
        InputIterable<Object> ids = wrap( "source", Arrays.<Object>asList( "8", "9", "10" ) );
        Groups groups = new Groups();
        Group firstGroup, secondGroup, thirdGroup;
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            int id = 0;
            mapper.put( iterator.next(), id++, firstGroup = groups.getOrCreate( "first" ) );
            mapper.put( iterator.next(), id++, secondGroup = groups.getOrCreate( "second" ) );
            mapper.put( iterator.next(), id++, thirdGroup = groups.getOrCreate( "third" ) );
        }
        mapper.prepare( ids, mock( Collector.class ), NONE );

        // WHEN/THEN
        assertEquals( 0L, mapper.get( "8", firstGroup ) );
        assertEquals( -1L, mapper.get( "8", secondGroup ) );
        assertEquals( -1L, mapper.get( "8", thirdGroup ) );

        assertEquals( -1L, mapper.get( "9", firstGroup ) );
        assertEquals( 1L, mapper.get( "9", secondGroup ) );
        assertEquals( -1L, mapper.get( "9", thirdGroup ) );

        assertEquals( -1L, mapper.get( "10", firstGroup ) );
        assertEquals( -1L, mapper.get( "10", secondGroup ) );
        assertEquals( 2L, mapper.get( "10", thirdGroup ) );
    }

    @Test
    public void shouldHandleManyGroups() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new LongEncoder(), new Radix.String(),
                NO_MONITOR );
        int size = 100;

        // WHEN
        for ( int i = 0; i < size; i++ )
        {
            mapper.put( i, i, new Group.Adapter( i, "" + i ) );
        }
        // null since this test should have been set up to not run into collisions
        mapper.prepare( null, mock( Collector.class ), NONE );

        // THEN
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( i, mapper.get( i, new Group.Adapter( i, "" + i ) ) );
        }
    }

    private class ValueGenerator implements InputIterable<Object>
    {
        private final int size;
        private final Factory<Object> generator;
        private final List<Object> values = new ArrayList<>();
        private final Set<Object> deduper = new HashSet<>();

        ValueGenerator( int size, Factory<Object> generator )
        {
            this.size = size;
            this.generator = generator;
        }

        @Override
        public InputIterator<Object> iterator()
        {
            if ( !values.isEmpty() )
            {
                return new SimpleInputIteratorWrapper<>( getClass().getSimpleName(), values.iterator() );
            }
            return new SimpleInputIterator<Object>( "" )
            {
                private int cursor;

                @Override
                protected Object fetchNextOrNull()
                {
                    if ( cursor < size )
                    {
                        while ( true )
                        {
                            Object value = generator.newInstance();
                            if ( deduper.add( value ) )
                            {
                                values.add( value );
                                cursor++;
                                return value;
                            }
                        }
                    }
                    return null;
                }
            };
        }

        @Override
        public boolean supportsMultiplePasses()
        {
            return false;
        }
    }

    private static enum ValueType
    {
        LONGS
        {
            @Override
            Encoder encoder()
            {
                return new LongEncoder();
            }

            @Override
            Radix radix()
            {
                return new Radix.Long();
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return new Factory<Object>()
                {
                    @Override
                    public Object newInstance()
                    {
                        return random.nextInt( 1_000_000_000 );
                    }
                };
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
            Radix radix()
            {
                return new Radix.String();
            }

            @Override
            Factory<Object> data( final Random random )
            {
                return new Factory<Object>()
                {
                    @Override
                    public Object newInstance()
                    {
                        return String.valueOf( random.nextInt( 1_000_000_000 ) );
                    }
                };
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
            Radix radix()
            {
                return new Radix.String();
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

        abstract Radix radix();

        abstract Factory<Object> data( Random random );
    }

    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );
    public final @Rule RepeatRule repeater = new RepeatRule();
}
