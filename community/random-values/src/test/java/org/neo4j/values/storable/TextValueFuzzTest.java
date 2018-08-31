/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.values.storable;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.RandomRule;

import static org.neo4j.values.storable.StringHelpers.assertConsistent;

public class TextValueFuzzTest
{
    @Rule
    public RandomRule random = new RandomRule();

    private static final int ITERATIONS = 1000;

    @Test
    public void shouldCompareTo()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(), random.nextString(),
                    ( t1, t2 ) -> Math.signum( t1.compareTo( t2 ) ) );
        }
    }

    @Test
    public void shouldAdd()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(), random.nextString(), TextValue::plus );
        }
    }

    @Test
    public void shouldComputeLength()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::length );
        }
    }

    @Test
    public void shouldReverse()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::reverse );
        }
    }

    @Test
    public void shouldTrim()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::trim );
        }
    }

}
