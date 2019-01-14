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
package org.neo4j.collection;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RawIteratorTest
{
    @Test
    public void shouldCreateSimpleRawIterator()
    {
        assertEquals( Collections.emptyList(), list( RawIterator.of() ) );
        assertEquals( Collections.singletonList( 1 ), list( RawIterator.of(1) ) );
        assertEquals( asList(1,2), list( RawIterator.of( 1,2 ) ) );
        assertEquals( asList(1,2,3), list( RawIterator.of( 1,2,3 ) ) );
    }

    public List<Integer> list( RawIterator<Integer, RuntimeException> iter )
    {
        LinkedList<Integer> out = new LinkedList<>();
        while ( iter.hasNext() )
        {
            out.add( iter.next() );
        }
        return out;
    }
}
