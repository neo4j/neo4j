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
package org.neo4j.collection.primitive.hopscotch;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.Primitive.VALUE_MARKER;
import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_H;

public class HopScotchHashingAlgorithmTest
{
    @Test
    public void shouldSupportIteratingThroughResize() throws Exception
    {
        // GIVEN
        int threshold = figureOutGrowthThreshold();
        TableGrowthAwareMonitor monitor = new TableGrowthAwareMonitor();
        PrimitiveLongSet set = new PrimitiveLongHashSet(
                new LongKeyTable<>( DEFAULT_H, VALUE_MARKER ), VALUE_MARKER, monitor );
        Set<Long> added = new HashSet<>();
        for ( int i = 0; i < threshold-1; i++ )
        {
            long value = i*3;
            set.add( value );
            added.add( value );
        }

        // WHEN
        PrimitiveLongIterator iterator = set.iterator();
        Set<Long> iterated = new HashSet<>();
        for ( int i = 0; i < threshold/2; i++ )
        {
            iterated.add( iterator.next() );
        }
        assertFalse( monitor.checkAndReset() );
        set.add( (threshold-1)*3 ); // will push it over the edge, to grow the table
        assertTrue( monitor.checkAndReset() );
        while ( iterator.hasNext() )
        {
            iterated.add( iterator.next() );
        }

        // THEN
        assertEquals( added, iterated );
    }

    private static class TableGrowthAwareMonitor extends Monitor.Adapter
    {
        private boolean grew;

        @Override
        public boolean tableGrew( int fromCapacity, int toCapacity, int currentSize )
        {
            return grew = true;
        }

        public boolean checkAndReset()
        {
            try
            {
                return grew;
            }
            finally
            {
                grew = false;
            }
        }
    }

    private int figureOutGrowthThreshold()
    {
        final AtomicBoolean grew = new AtomicBoolean();
        Monitor monitor = new Monitor.Adapter()
        {
            @Override
            public boolean tableGrew( int fromCapacity, int toCapacity, int currentSize )
            {
                grew.set( true );
                return true;
            }
        };
        try ( PrimitiveLongSet set = new PrimitiveLongHashSet(
                new LongKeyTable<>( DEFAULT_H, VALUE_MARKER ), VALUE_MARKER, monitor ) )
        {
            int i = 0;
            for ( i = 0; !grew.get(); i++ )
            {
                set.add( i*3 );
            }
            return i;
        }
    }
}
