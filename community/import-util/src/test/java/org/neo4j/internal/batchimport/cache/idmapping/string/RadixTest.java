/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class RadixTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldKeepNullValuesInSeparateCounter()
    {
        // given
        Radix radix = Radix.LONG.newInstance();
        long nullValue = EncodingIdMapper.GAP_VALUE;

        // when
        int expectedNumNullValues = 0;
        for ( int i = 0; i < 100; i++ )
        {
            boolean realValue = random.nextBoolean();
            radix.registerRadixOf( realValue ? random.nextLong( 1, 10_000 ) : nullValue );
            if ( !realValue )
            {
                expectedNumNullValues++;
            }
        }

        // then
        assertEquals( expectedNumNullValues, radix.getNullCount() );
    }
}
