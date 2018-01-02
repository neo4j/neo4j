/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrimitiveIntStackTest
{
    @Test
    public void shouldPushAndPollSomeEntities() throws Exception
    {
        // GIVEN
        PrimitiveIntStack stack = new PrimitiveIntStack( 6 );

        // WHEN/THEN
        assertTrue( stack.isEmpty() );
        assertEquals( -1, stack.poll() );

        stack.push( 123 );
        assertFalse( stack.isEmpty() );

        stack.push( 456 );
        assertFalse( stack.isEmpty() );
        assertEquals( 456, stack.poll() );

        assertFalse( stack.isEmpty() );
        assertEquals( 123, stack.poll() );

        assertTrue( stack.isEmpty() );
        assertEquals( -1, stack.poll() );
    }

    @Test
    public void shouldGrowArray() throws Exception
    {
        // GIVEN
        PrimitiveIntStack stack = new PrimitiveIntStack( 5 );

        // WHEN
        for ( int i = 0; i <= 7; i++ )
        {
            stack.push( i );
        }

        // THEN
        for ( int i = 7; i >= 0; i-- )
        {
            assertFalse( stack.isEmpty() );
            assertEquals( i, stack.poll() );
        }
        assertTrue( stack.isEmpty() );
        assertEquals( -1, stack.poll() );
    }
}
