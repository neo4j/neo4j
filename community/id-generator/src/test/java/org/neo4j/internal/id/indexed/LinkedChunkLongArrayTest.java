/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.id.indexed;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LinkedChunkLongArrayTest
{
    @Test
    void shouldFillSeveralChunks()
    {
        // given
        LinkedChunkLongArray values = new LinkedChunkLongArray( 10 );

        // when
        int numValues = 100;
        for ( int i = 0; i < numValues; i++ )
        {
            values.add( i );
            assertThat( values.size() ).isEqualTo( i + 1 );
        }

        // then
        MutableLong lastSeenValue = new MutableLong( -1 );
        values.accept( id ->
        {
            assertThat( id ).isEqualTo( lastSeenValue.longValue() + 1 );
            lastSeenValue.setValue( id );
        } );
        assertThat( lastSeenValue.longValue() ).isEqualTo( numValues - 1 );
    }
}
