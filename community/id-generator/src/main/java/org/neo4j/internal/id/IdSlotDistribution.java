/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id;

import java.util.Objects;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.Preconditions;

import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;

/**
 * Defines which slot sizes for IDs to use, e.g. a slot size of 4 contains IDs where the ID + the 3 next IDs are available.
 */
public interface IdSlotDistribution
{
    IdSlotDistribution SINGLE_IDS = capacity -> new Slot[]{new Slot( capacity, 1 )};

    static IdSlotDistribution evenSlotDistribution( int... slotSizes )
    {
        return evenSlotDistribution( IDS_PER_ENTRY, slotSizes );
    }

    static IdSlotDistribution evenSlotDistribution( int idsPerEntry, int... slotSizes )
    {
        return new BaseIdSlotDistribution( idsPerEntry )
        {
            @Override
            public Slot[] slots( int capacity )
            {
                Slot[] slots = new Slot[slotSizes.length];
                int capacityPerSlot = nearestHigherPowerOfTwo( capacity / slotSizes.length );
                for ( int i = 0; i < slotSizes.length; i++ )
                {
                    slots[i] = new Slot( capacityPerSlot, slotSizes[i] );
                }
                return slots;
            }

            private int nearestHigherPowerOfTwo( int value )
            {
                return Integer.bitCount( value ) == 1
                       ? value
                       : Integer.highestOneBit( value ) << 1;
            }
        };
    }

    static IdSlotDistribution diminishingSlotDistribution( int... slotSizes )
    {
        return diminishingSlotDistribution( IDS_PER_ENTRY, slotSizes );
    }

    static IdSlotDistribution diminishingSlotDistribution( int idsPerEntry, int... slotSizes )
    {
        return new BaseIdSlotDistribution( idsPerEntry )
        {
            @Override
            public Slot[] slots( int capacity )
            {
                Slot[] slots = new Slot[slotSizes.length];
                for ( int i = 0; i < slotSizes.length; i++ )
                {
                    slots[i] = new Slot( capacity / (1 << (i + 1)), slotSizes[i] );
                }
                return slots;
            }
        };
    }

    static IdSlotDistribution allWithSameCapacity( int... slotSizes )
    {
        return allWithSameCapacity( IDS_PER_ENTRY, slotSizes );
    }

    static IdSlotDistribution allWithSameCapacity( int idsPerEntry, int... slotSizes )
    {
        return new BaseIdSlotDistribution( idsPerEntry )
        {
            @Override
            public Slot[] slots( int capacity )
            {
                Slot[] slots = new Slot[slotSizes.length];
                for ( int i = 0; i < slotSizes.length; i++ )
                {
                    slots[i] = new Slot( capacity, slotSizes[i] );
                }
                return slots;
            }
        };
    }

    static int[] powerTwoSlotSizesDownwards( int highSlotSize )
    {
        Preconditions.checkArgument( Integer.bitCount( highSlotSize ) == 1, "Requires a power of two" );
        int[] slots = new int[Integer.numberOfTrailingZeros( highSlotSize ) + 1];
        for ( int i = 0; i < slots.length; i++ )
        {
            slots[i] = 1 << i;
        }
        return slots;
    }

    Slot[] slots( int capacity );

    /**
     * This affects high ID and how {@link IdGenerator#nextConsecutiveIdRange(int, CursorContext)} decides when to cross a store "page"
     */
    default int idsPerEntry()
    {
        return IDS_PER_ENTRY;
    }

    class Slot
    {
        private final int capacity;
        private final int slotSize;

        public Slot( int capacity, int slotSize )
        {
            this.capacity = capacity;
            this.slotSize = slotSize;
        }

        public int capacity()
        {
            return capacity;
        }

        public int slotSize()
        {
            return slotSize;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Slot slot = (Slot) o;
            return capacity == slot.capacity && slotSize == slot.slotSize;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( capacity, slotSize );
        }

        @Override
        public String toString()
        {
            return "Slot{" + "capacity=" + capacity + ", slotSize=" + slotSize + '}';
        }
    }

    abstract class BaseIdSlotDistribution implements IdSlotDistribution
    {
        private final int idsPerEntry;

        BaseIdSlotDistribution( int idsPerEntry )
        {
            this.idsPerEntry = idsPerEntry;
        }

        @Override
        public int idsPerEntry()
        {
            return idsPerEntry;
        }
    }
}
