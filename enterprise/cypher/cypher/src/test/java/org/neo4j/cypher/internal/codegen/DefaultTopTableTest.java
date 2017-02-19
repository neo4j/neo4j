/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class DefaultTopTableTest
{
    private static Long[] testValues = new Long[] {
        7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L
    };

    @Test
    public void shouldOrderValuesCorrectly()
    {
        DefaultTopTable table = new DefaultTopTable( 5 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        table.sort();

        Iterator<Object> iterator = table.iterator();

        for ( int i = 0; i < 5; i++ )
        {
            assertTrue( iterator.hasNext() );
            long value = (long) iterator.next();
            assertEquals( i, value );
        }
        assertFalse( iterator.hasNext() );
    }
}
