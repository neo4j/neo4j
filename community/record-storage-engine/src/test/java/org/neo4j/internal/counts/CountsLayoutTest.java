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

import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;

class CountsLayoutTest
{
    @Test
    void shouldDifferentiateBetweenRelationshipKeys()
    {
        // given
        CountsLayout layout = new CountsLayout();
        TreeSet<CountsKey> keys = new TreeSet<>( layout );

        // when
        for ( int s = -1; s < 10; s++ )
        {
            for ( int t = -1; t < 10; t++ )
            {
                for ( int e = -1; e < 10; e++ )
                {
                    // then
                    CountsKey key = relationshipKey( s, t, e );
                    assertTrue( keys.add( key ) );
                }
            }
        }
    }
}
