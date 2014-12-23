/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.function.Factory;
import org.neo4j.function.primitive.PrimitiveIntPredicate;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Group;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Resource.EMPTY;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ANY_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;

public class EncodingIdMapperTest
{
    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( NumberArrayFactory.AUTO );
        ResourceIterable<Object> ids = new ResourceIterable<Object>()
        {
            @Override
            public ResourceIterator<Object> iterator()
            {
                return new PrefetchingResourceIterator<Object>()
                {
                    private int i;

                    @Override
                    protected Object fetchNextOrNull()
                    {
                        return i++ < 300_000
                                ? "" + i
                                : null;
                    }

                    @Override
                    public void close()
                    {   // Nothing to close
                    }
                };
            }
        };

        // WHEN
        long index = 0;
        for ( Object id : ids )
        {
            idMapper.put( id, index++, GLOBAL );
        }
        idMapper.prepare( ids );
        MemoryStatsVisitor memoryStats = new GatheringMemoryStatsVisitor();
        idMapper.visitMemoryStats( memoryStats );

        // THEN
        for ( Object id : ids )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            if ( idMapper.get( id, ANY_GROUP ) == -1 )
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
        idMapper.prepare( null );

        // WHEN
        long id = idMapper.get( "123", ANY_GROUP );

        // THEN
        assertEquals( -1L, id );
    }

    @Test
    public void shouldEncodeShortStrings() throws Exception
    {
        // GIVEN
        IdMapper mapper = IdMappers.strings( NumberArrayFactory.AUTO );

        // WHEN
        mapper.put( "123", 0, GLOBAL );
        mapper.put( "456", 1, GLOBAL );
        mapper.prepare( null );

        // THEN
        assertEquals( 1L, mapper.get( "456", ANY_GROUP ) );
        assertEquals( 0L, mapper.get( "123", ANY_GROUP ) );
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
                size*2, processorsForSorting /*1-7*/ );

        // WHEN
        ResourceIterable<Object> values = new ValueGenerator( size, type.data( random ) );
        {
            int id = 0;
            for ( Object value : values )
            {
                mapper.put( value, id++, GLOBAL );
            }
        }

        try
        {
            mapper.prepare( values );

            // THEN
            int id = 0;
            for ( Object value : values )
            {
                assertEquals( "Expected " + value + " to map to " + id + ", seed:" + seed, id++,
                        mapper.get( value, ANY_GROUP ) );
            }
        }
        catch ( Throwable e )
        {
            throw Exceptions.withMessage( e, e.getMessage() + ", seed:" + seed );
        }
    }

    @Test
    public void shouldRefuseCollisionsBasedOnSameInputId() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String() );
        ResourceIterable<Object> ids =
                IteratorUtil.<Object>resourceIterable( Arrays.<Object>asList( "10", "9", "10" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, GLOBAL );
            }
        }

        // WHEN
        try
        {
            mapper.prepare( ids );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
            assertTrue( e.getMessage().contains( "10" ) );
            e.printStackTrace();
        }
    }

    @Test
    public void shouldCopyWithCollisionsBasedOnDifferentInputIds() throws Exception
    {
        // GIVEN
        Encoder encoder = mock( Encoder.class );
        when( encoder.encode( any() ) ).thenReturn( 12345L );
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, encoder, new Radix.String() );
        ResourceIterable<Object> ids =
                IteratorUtil.<Object>resourceIterable( Arrays.<Object>asList( "10", "9" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            for ( int i = 0; iterator.hasNext(); i++ )
            {
                mapper.put( iterator.next(), i, GLOBAL );
            }
        }

        // WHEN
        mapper.prepare( ids );

        // THEN
        assertEquals( 0L, mapper.get( "10", ANY_GROUP ) );
        assertEquals( 1L, mapper.get( "9", ANY_GROUP ) );
    }

    @Test
    public void shouldBeAbleToHaveDuplicateInputIdButInDifferentGroups() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String() );
        ResourceIterable<Object> ids =
                IteratorUtil.<Object>resourceIterable( Arrays.<Object>asList( "10", "9", "10" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            int group = 0, id = 0;
            // group 0
            Group group0 = group( group++, "zero" );
            mapper.put( iterator.next(), id++, group0 );
            mapper.put( iterator.next(), id++, group0 );
            // group 1
            Group group1 = group( group, "one" );
            mapper.put( iterator.next(), id++, group1 );
        }
        mapper.prepare( ids );

        // WHEN/THEN
        assertEquals( 0L, mapper.get( "10", groups( 0 ) ) );
        assertEquals( 1L, mapper.get( "9", groups( 0 ) ) );
        assertEquals( 2L, mapper.get( "10", groups( 1 ) ) );
    }

    @Test
    public void shouldPreventCollisionGetFromManyGroups() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String() );
        ResourceIterable<Object> ids =
                IteratorUtil.<Object>resourceIterable( Arrays.<Object>asList( "9", "10", "10" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            int group = 0, id = 0;
            // group 0
            Group group0 = group( group++, "zero" );
            mapper.put( iterator.next(), id++, group0 );
            mapper.put( iterator.next(), id++, group0 );
            // group 1
            Group group1 = group( group, "one" );
            mapper.put( iterator.next(), id++, group1 );
        }
        mapper.prepare( ids );

        // WHEN/THEN
        try
        {
            mapper.get( "10", ANY_GROUP );
            fail( "Should fail" );
        }
        catch ( IllegalStateException e )
        {
            // Good
        }
    }

    @Test
    public void shouldOnlyFindInputIdsInSpecificGroup() throws Exception
    {
        // GIVEN
        IdMapper mapper = new EncodingIdMapper( NumberArrayFactory.HEAP, new StringEncoder(), new Radix.String() );
        ResourceIterable<Object> ids =
                IteratorUtil.<Object>resourceIterable( Arrays.<Object>asList( "8", "9", "10" ) );
        try ( ResourceIterator<Object> iterator = ids.iterator() )
        {
            int group = 0, id = 0;
            mapper.put( iterator.next(), id++, group( group++, "first" ) );
            mapper.put( iterator.next(), id++, group( group++, "second" ) );
            mapper.put( iterator.next(), id++, group( group++, "third" ) );
        }
        mapper.prepare( ids );

        // WHEN/THEN
        assertEquals( 0L, mapper.get( "8", groups( 0 ) ) );
        assertEquals( -1L, mapper.get( "8", groups( 1 ) ) );
        assertEquals( -1L, mapper.get( "8", groups( 2 ) ) );
        assertEquals( 0L, mapper.get( "8", groups( 0, 1, 2 ) ) );

        assertEquals( -1L, mapper.get( "9", groups( 0 ) ) );
        assertEquals( 1L, mapper.get( "9", groups( 1 ) ) );
        assertEquals( -1L, mapper.get( "9", groups( 2 ) ) );
        assertEquals( 1L, mapper.get( "9", groups( 0, 1, 2 ) ) );

        assertEquals( -1L, mapper.get( "10", groups( 0 ) ) );
        assertEquals( -1L, mapper.get( "10", groups( 1 ) ) );
        assertEquals( 2L, mapper.get( "10", groups( 2 ) ) );
        assertEquals( 2L, mapper.get( "10", groups( 0, 1, 2 ) ) );
    }

    private Group group( int id, final String name )
    {
        return new Group.Adapter( id, name );
    }

    private PrimitiveIntPredicate groups( int... groups )
    {
        PrimitiveIntSet set = Primitive.intSet( groups.length );
        for ( int group : groups )
        {
            set.add( group );
        }
        return set;
    }

    private class ValueGenerator implements ResourceIterable<Object>
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
        public ResourceIterator<Object> iterator()
        {
            if ( !values.isEmpty() )
            {
                return resourceIterator( values.iterator(), EMPTY );
            }
            return resourceIterator( new PrefetchingIterator<Object>()
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
            }, EMPTY );
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
