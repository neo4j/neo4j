/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultTopTableTest
{
    private static Long[] testValues = new Long[]{7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L};

    private static long[] expectedValues = new long[]{0L, 1L, 2L, 3L, 4L, 4L, 5L, 6L, 7L, 8L, 9L};

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleAddingMoreValuesThanCapacity()
    {
        DefaultTopTable table = new DefaultTopTable( 7 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        table.sort();

        Iterator<Object> iterator = table.iterator();

        for ( int i = 0; i < 7; i++ )
        {
            assertTrue( iterator.hasNext() );
            long value = (long) iterator.next();
            assertEquals( expectedValues[i], value );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldHandleWhenNotCompletelyFilledToCapacity()
    {
        DefaultTopTable table = new DefaultTopTable( 20 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        table.sort();

        Iterator<Object> iterator = table.iterator();

        for ( int i = 0; i < testValues.length; i++ )
        {
            assertTrue( iterator.hasNext() );
            long value = (long) iterator.next();
            assertEquals( expectedValues[i], value );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldHandleWhenEmpty()
    {
        DefaultTopTable table = new DefaultTopTable( 10 );

        table.sort();

        Iterator<Object> iterator = table.iterator();

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldThrowOnInitializeToZeroCapacity()
    {
        exception.expect( IllegalArgumentException.class );
        new DefaultTopTable( 0 );
    }

    @Test
    public void shouldThrowOnInitializeToNegativeCapacity()
    {
        exception.expect( IllegalArgumentException.class );
        new DefaultTopTable( -1 );
    }

    @Test
    public void shouldThrowOnSortNotCalledBeforeIterator()
    {
        DefaultTopTable table = new DefaultTopTable( 5 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        // We forgot to call sort() here...

        exception.expect( IllegalStateException.class );
        table.iterator();
    }
}
