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
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.collection.primitive.Primitive.intSet;

public class StringIdMapperTest
{
    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = new StringIdMapper( LongArrayFactory.AUTO );
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
        IdMapper idMapper = new StringIdMapper( LongArrayFactory.AUTO );
        idMapper.prepare( Arrays.asList() );

        // WHEN
        long id = idMapper.get( "missing" );

        // THEN
        assertEquals( -1L, id );
    }

    @Ignore( "TODO pending fix issue in ParallelSort" )
    @Test
    public void shouldEncodeShortStrings() throws Exception
    {
        // GIVEN
        StringIdMapper mapper = new StringIdMapper( LongArrayFactory.AUTO );

        // WHEN
        mapper.put( "123", 1 );
        mapper.put( "456", 2 );
        mapper.prepare( null );

        // THEN
        assertEquals( 2L, mapper.get( "456" ) );
        assertEquals( 1L, mapper.get( "123" ) );
    }

    @Ignore( "TODO pending fix issue in ParallelSort" )
    @Test
    public void shouldEncodeSmallSetOfRandomData() throws Throwable
    {
        // GIVEN
        StringIdMapper mapper = new StringIdMapper( LongArrayFactory.AUTO, random.nextInt( 7 ) + 1 /*1-7*/ );
        int size = random.nextInt( 1_000 ) + 2;

        // WHEN
        List<Object> values = new ArrayList<>();
        PrimitiveIntSet alreadyGeneratedValues = intSet( size );
        for ( int i = 0; i < size; i++ )
        {
            int candidate = random.nextInt( 10_000 );
            if ( !alreadyGeneratedValues.add( candidate ) )
            {
                i--;
                continue;
            }

            String value = String.valueOf( candidate );
            mapper.put( value, i );
            values.add( value );
        }
        try
        {
            mapper.prepare( values );

            // THEN
            int i = 0;
            for ( Object value : values )
            {
                assertEquals( "Expected " + value + " to map to " + i + ", seed:" + seed, i++, mapper.get( value ) );
            }
        }
        catch ( Throwable e )
        {
            throw Exceptions.withMessage( e, e.getMessage() + ", seed:" + seed );
        }
    }

    private final long seed = currentTimeMillis(); // 1415196442896L;
    private final Random random = new Random( seed );
}
