/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class PortIteratorTest
{
    @Test
    void shouldCountUp()
    {
        PortIterator portIterator = new PortIterator( new int[]{6000, 6005} );

        assertEquals( 6000, (int) portIterator.next() );
        assertEquals( 6001, (int) portIterator.next() );
        assertEquals( 6002, (int) portIterator.next() );
        assertEquals( 6003, (int) portIterator.next() );
        assertEquals( 6004, (int) portIterator.next() );
        assertEquals( 6005, (int) portIterator.next() );
        assertFalse( portIterator.hasNext() );
    }

    @Test
    void shouldCountDown()
    {
        PortIterator portIterator = new PortIterator( new int[]{6005, 6000} );

        assertEquals( 6005, (int) portIterator.next() );
        assertEquals( 6004, (int) portIterator.next() );
        assertEquals( 6003, (int) portIterator.next() );
        assertEquals( 6002, (int) portIterator.next() );
        assertEquals( 6001, (int) portIterator.next() );
        assertEquals( 6000, (int) portIterator.next() );
        assertFalse( portIterator.hasNext() );
    }

    @Test
    void shouldNotSupportRemove()
    {
        try
        {
            new PortIterator( new int[]{6000, 6005} ).remove();
            fail("Should have thrown exception");
        }
        catch ( UnsupportedOperationException e )
        {
            // expected
        }
    }
}
