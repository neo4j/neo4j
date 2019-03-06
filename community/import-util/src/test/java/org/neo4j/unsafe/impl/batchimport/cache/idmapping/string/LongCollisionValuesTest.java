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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.CHUNKED_FIXED_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.OFF_HEAP;

@RunWith( Parameterized.class )
public class LongCollisionValuesTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Parameters
    public static Collection<NumberArrayFactory> data()
    {
        return Arrays.asList( HEAP, OFF_HEAP, AUTO_WITHOUT_PAGECACHE, CHUNKED_FIXED_SIZE );
    }

    @Parameter( 0 )
    public NumberArrayFactory factory;

    @Test
    public void shouldStoreAndLoadLongs()
    {
        // given
        try ( LongCollisionValues values = new LongCollisionValues( factory, 100 ) )
        {
            // when
            long[] offsets = new long[100];
            long[] longs = new long[offsets.length];
            for ( int i = 0; i < offsets.length; i++ )
            {
                long value = random.nextLong( Long.MAX_VALUE );
                offsets[i] = values.add( value );
                longs[i] = value;
            }

            // then
            for ( int i = 0; i < offsets.length; i++ )
            {
                assertEquals( longs[i], (long) values.get( offsets[i] ) );
            }
        }
    }
}
