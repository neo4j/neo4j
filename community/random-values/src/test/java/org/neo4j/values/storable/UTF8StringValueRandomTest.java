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

@ExtendWith( RandomExtension.class )
class UTF8StringValueRandomTest
{
    @Inject
    RandomRule random;

    @Test
    void shouldCompareToRandomAlphanumericString()
    {
        for ( int i = 0; i < 100; i++ )
        {
            String string1 = random.nextAlphaNumericString();
            String string2 = random.nextAlphaNumericString();
            UTF8StringValueTest.assertCompareTo( string1, string2 );
        }
    }

    @Test
    void shouldCompareToAsciiString()
    {
        for ( int i = 0; i < 100; i++ )
        {
            String string1 = random.nextAsciiString();
            String string2 = random.nextAsciiString();
            UTF8StringValueTest.assertCompareTo( string1, string2 );
        }
    }

    @Test
    void shouldCompareBasicMultilingualPlaneString()
    {
        for ( int i = 0; i < 100; i++ )
        {
            String string1 = random.nextBasicMultilingualPlaneString();
            String string2 = random.nextBasicMultilingualPlaneString();
            UTF8StringValueTest.assertCompareTo( string1, string2 );
        }
    }

    @Disabled( "Comparing strings with higher than 16 bits code points is known to be inconsistent between StringValue and UTF8StringValue" )
    @Test
    void shouldCompareToRandomString()
    {
        for ( int i = 0; i < 100; i++ )
        {
            String string1 = random.nextString();
            String string2 = random.nextString();
            UTF8StringValueTest.assertCompareTo( string1, string2 );
        }
    }
}
