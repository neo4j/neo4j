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

import org.neo4j.function.Factory;
import org.neo4j.register.Register.IntRegister;

import static java.lang.Math.pow;

import static org.neo4j.register.Registers.newIntRegister;

/**
 * Calculates and keeps radix counts. Uses a {@link RadixCalculator} to calculate an integer radix value
 * from a long value.
 */
public abstract class Radix
{
    public static final Factory<Radix> LONG = new Factory<Radix>()
    {
        @Override
        public Radix newInstance()
        {
            return new Radix.Long();
        }
    };

    public static final Factory<Radix> STRING = new Factory<Radix>()
    {
        @Override
        public Radix newInstance()
        {
            return new Radix.String();
        }
    };

    protected final int[] radixIndexCount = new int[(int) pow( 2, RadixCalculator.RADIX_BITS - 1 )];

    public int registerRadixOf( long value )
    {
        int radix = calculator().radixOf( value );
        radixIndexCount[radix]++;
        return radix;
    }

    public int[] getRadixIndexCounts()
    {
        return radixIndexCount;
    }

    public abstract RadixCalculator calculator();

    @Override
    public java.lang.String toString()
    {
        return Radix.class.getSimpleName() + "." + getClass().getSimpleName();
    }

    public static class String extends Radix
    {
        private final RadixCalculator calculator;

        public String()
        {
            this.calculator = new RadixCalculator.String();
        }

        @Override
        public RadixCalculator calculator()
        {
            return calculator;
        }
    }

    public static class Long extends Radix
    {
        private final IntRegister radixShift;
        private final RadixCalculator calculator;

        public Long()
        {
            this.radixShift = newIntRegister( 0 );
            this.calculator = new RadixCalculator.Long( radixShift );
        }

        @Override
        public RadixCalculator calculator()
        {
            return calculator;
        }

        @Override
        public int registerRadixOf( long value )
        {
            radixOverflow( value );
            return super.registerRadixOf( value );
        }

        private void radixOverflow( long val )
        {
            long shiftVal = ((val & ~RadixCalculator.LENGTH_BITS) >> (RadixCalculator.RADIX_BITS - 1 + radixShift.read()));
            if ( shiftVal > 0 )
            {
                while ( shiftVal > 0 )
                {
                    radixShift.increment( 1 );
                    compressRadixIndex();
                    shiftVal = shiftVal >> 1;
                }
            }
        }

        private void compressRadixIndex()
        {
            for ( int i = 0; i < radixIndexCount.length / 2; i++ )
            {
                radixIndexCount[i] = radixIndexCount[2 * i] + radixIndexCount[2 * i + 1];
            }
            for ( int i = radixIndexCount.length / 2; i < radixIndexCount.length; i++ )
            {
                radixIndexCount[i] = 0;
            }
        }
    }
}
