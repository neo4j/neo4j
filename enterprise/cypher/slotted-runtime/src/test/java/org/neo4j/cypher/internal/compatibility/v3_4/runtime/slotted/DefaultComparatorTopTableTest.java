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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.util.Comparator;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableRuleMigrationSupport
public class DefaultComparatorTopTableTest
{
    private static Long[] testValues = new Long[]{7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L};

    private static long[] expectedValues = new long[]{0L, 1L, 2L, 3L, 4L, 4L, 5L, 6L, 7L, 8L, 9L};

    private static final Comparator<Long> comparator = Long::compare;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleAddingMoreValuesThanCapacity()
    {
        DefaultComparatorTopTable table = new DefaultComparatorTopTable( comparator, 7 );
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
        DefaultComparatorTopTable table = new DefaultComparatorTopTable( comparator, 20 );
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
        DefaultComparatorTopTable table = new DefaultComparatorTopTable( comparator, 10 );

        table.sort();

        Iterator<Object> iterator = table.iterator();

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldThrowOnInitializeToZeroCapacity()
    {
        exception.expect( IllegalArgumentException.class );
        new DefaultComparatorTopTable( comparator, 0 );
    }

    @Test
    public void shouldThrowOnInitializeToNegativeCapacity()
    {
        exception.expect( IllegalArgumentException.class );
        new DefaultComparatorTopTable( comparator, -1 );
    }

    @Test
    public void shouldThrowOnSortNotCalledBeforeIterator()
    {
        DefaultComparatorTopTable table = new DefaultComparatorTopTable( comparator, 5 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        // We forgot to call sort() here...

        exception.expect( IllegalStateException.class );
        table.iterator();
    }
}
