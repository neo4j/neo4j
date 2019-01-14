/*
 * Copyright (c) 2002-2019 "Neo4j,"
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


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.neo4j.values.storable.StringHelpers.assertConsistent;

@ExtendWith( RandomExtension.class )
class TextValueFuzzTest
{
    @Inject
    private RandomRule random;

    private static final int ITERATIONS = 1000;

    @Disabled(
            "we have decided to stick with String::compareTo under the hood which doesn't respect code point order " +
            "whenever the code point doesn't fit 16bits" )
    @Test
    void shouldCompareToForAllValidStrings()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(), random.nextString(),
                    ( t1, t2 ) -> Math.signum( t1.compareTo( t2 ) ) );
        }
    }

    @Test
    void shouldCompareToForAllStringsInBasicMultilingualPlane()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextBasicMultilingualPlaneString(), random.nextBasicMultilingualPlaneString(),
                    ( t1, t2 ) -> Math.signum( t1.compareTo( t2 ) ) );
        }
    }

    @Test
    void shouldAdd()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(), random.nextString(), TextValue::plus );
        }
    }

    @Test
    void shouldComputeLength()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::length );
        }
    }

    @Test
    void shouldReverse()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::reverse );
        }
    }

    @Test
    void shouldTrim()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertConsistent( random.nextString(),  TextValue::trim );
        }
    }

    @Test
    void shouldHandleStringPredicates()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            String value = random.nextString();
            String other;
            if ( random.nextBoolean() )
            {
                other = value;
            }
            else
            {
                other = random.nextString();
            }

            assertConsistent( value, other, TextValue::startsWith );
            assertConsistent( value, other, TextValue::endsWith );
            assertConsistent( value, other, TextValue::contains );
        }
    }

}
