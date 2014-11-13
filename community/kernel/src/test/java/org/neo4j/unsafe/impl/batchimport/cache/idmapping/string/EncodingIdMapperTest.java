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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Factory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EncodingIdMapperTest
{
    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( LongArrayFactory.AUTO );
        Iterable<Object> ids = new Iterable<Object>()
        {
            @Override
            public Iterator<Object> iterator()
            {
                return new PrefetchingIterator<Object>()
                {
                    private int i;

                    @Override
                    protected Object fetchNextOrNull()
                    {
                        return i++ < 300_000
                                ? "" + i
                                : null;
                    }
                };
            }
        };

        // WHEN
        long index = 0;
        for ( Object id : ids )
        {
            idMapper.put( id, index++ );
        }
        idMapper.prepare( ids );
        MemoryStatsVisitor memoryStats = new GatheringMemoryStatsVisitor();
        idMapper.visitMemoryStats( memoryStats );

        // THEN
        for ( Object id : ids )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            if ( idMapper.get( id ) == -1 )
            {
                fail( "Couldn't find " + id + " even though I added it just previously" );
            }
        }
    }

    @Test
    public void shouldReturnExpectedValueForNotFound() throws Exception
    {
        // GIVEN
        IdMapper idMapper = IdMappers.strings( LongArrayFactory.AUTO );
        idMapper.prepare( Arrays.asList() );

        // WHEN
        long id = idMapper.get( "123" );

        // THEN
        assertEquals( -1L, id );
    }

    @Test
    public void shouldEncodeShortStrings() throws Exception
    {
        // GIVEN
        IdMapper mapper = IdMappers.strings( LongArrayFactory.AUTO );

        // WHEN
        mapper.put( "123", 0 );
        mapper.put( "456", 1 );
        mapper.prepare( null );

        // THEN
        assertEquals( 1L, mapper.get( "456" ) );
        assertEquals( 0L, mapper.get( "123" ) );
    }

    @Repeat( times = 10 )
    @Test
    public void shouldEncodeSmallSetOfRandomData() throws Throwable
    {
        // GIVEN
        int processorsForSorting = random.nextInt( 7 ) + 1;
        int size = random.nextInt( 10_000 ) + 2;
        boolean stringOrLong = random.nextBoolean();
        IdMapper mapper = new EncodingIdMapper( LongArrayFactory.HEAP,
                stringOrLong ? new StringEncoder() : new LongEncoder(),
                stringOrLong ? new Radix.String() : new Radix.Long(),
                size*2, processorsForSorting /*1-7*/ );

        // WHEN
        Iterable<Object> values = new ValueGenerator( size, stringOrLong ? new StringValues() : new LongValues() );
        {
            int id = 0;
            for ( Object value : values )
            {
                mapper.put( value, id++ );
            }
        }

        try
        {
            mapper.prepare( values );

            // THEN
            int id = 0;
            for ( Object value : values )
            {
                assertEquals( "Expected " + value + " to map to " + id + ", seed:" + seed, id++, mapper.get( value ) );
            }
        }
        catch ( Throwable e )
        {
            throw Exceptions.withMessage( e, e.getMessage() + ", seed:" + seed );
        }
    }

    private class LongValues implements Factory<Object>
    {
        @Override
        public Object newInstance()
        {
            return random.nextInt( 1_000_000_000 );
        }
    }

    private class StringValues implements Factory<Object>
    {
        @Override
        public Object newInstance()
        {
            return String.valueOf( random.nextInt( 1_000_000_000 ) );
        }
    }

    private class ValueGenerator implements Iterable<Object>
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
        public Iterator<Object> iterator()
        {
            if ( !values.isEmpty() )
            {
                return values.iterator();
            }
            return new PrefetchingIterator<Object>()
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
    }

    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );
    public final @Rule RepeatRule repeater = new RepeatRule();
}
