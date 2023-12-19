/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class PortIteratorTest
{
    @Test
    public void shouldCountUp()
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
    public void shouldCountDown()
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
    public void shouldNotSupportRemove()
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
