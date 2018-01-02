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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.register.Register;

/**
 * Calculates the radix of {@link Long} values.
 */
public abstract class RadixCalculator
{
    protected static final int RADIX_BITS = 24;
    protected static final long LENGTH_BITS = 0xFE000000_00000000L;
    protected static final int LENGTH_MASK = (int) (LENGTH_BITS >>> (64 - RADIX_BITS));
    protected static final int HASHCODE_MASK = (int) (0x00FFFF00_00000000L >>> (64 - RADIX_BITS));

    public abstract int radixOf( long value );

    /**
     * Radix optimized for strings encoded into long by {@link StringEncoder}.
     */
    public static class String extends RadixCalculator
    {
        @Override
        public int radixOf( long value )
        {
            int index = (int) (value >>> (64 - RADIX_BITS));
            index = (((index & LENGTH_MASK) >>> 1) | (index & HASHCODE_MASK));
            return index;
        }
    }

    /**
     * Radix optimized for strings encoded into long by {@link LongEncoder}.
     */
    public static class Long extends RadixCalculator
    {
        private final Register.Int.In radixShift;

        public Long( Register.Int.In radixShift )
        {
            this.radixShift = radixShift;
        }

        @Override
        public int radixOf( long value )
        {
            long val1 = (value & ~LENGTH_BITS);
            val1 = val1 >>> radixShift.read();
            int index = (int) val1;
            return index;
        }
    }
}
