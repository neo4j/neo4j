/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.Random;

import org.junit.Test;

import static java.lang.Math.abs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.util.NumberUtil.haveSameSign;
import static org.neo4j.kernel.impl.util.NumberUtil.signOf;

public class NumberUtilTest
{
    @Test
    public void shouldSeeZeroAsPositive() throws Exception
    {
        assertTrue( signOf( 0 ) );
    }

    @Test
    public void shouldSeeArbitraryPositiveValuesAsPositive() throws Exception
    {
        Random random = new Random();
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( signOf( abs( random.nextLong() ) ) );
        }
        assertTrue( signOf( Long.MAX_VALUE ) );
    }

    @Test
    public void shouldSeeArbitraryNegativeValuesAsNegative() throws Exception
    {
        Random random = new Random();
        for ( int i = 0; i < 100; i++ )
        {
            assertFalse( signOf( -abs( random.nextLong() ) ) );
        }
        assertFalse( signOf( Long.MIN_VALUE ) );
    }

    @Test
    public void shouldSeeNumbersOfSameSignAsSameSign() throws Exception
    {
        Random random = new Random();
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( haveSameSign( abs( random.nextLong() ), abs( random.nextLong() ) ) );
        }
    }

    @Test
    public void shouldSeeNumbersOfDifferentSignAsDifferentSign() throws Exception
    {
        Random random = new Random();
        for ( int i = 0; i < 100; i++ )
        {
            assertFalse( haveSameSign( abs( random.nextLong() ), -abs( random.nextLong() ) ) );
            assertFalse( haveSameSign( -abs( random.nextLong() ), abs( random.nextLong() ) ) );
        }
    }
}
