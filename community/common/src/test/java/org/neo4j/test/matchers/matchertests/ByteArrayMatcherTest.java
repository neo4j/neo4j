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
package org.neo4j.test.matchers.matchertests;

import org.junit.Test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.matchers.ByteArrayMatcher.byteArray;

public class ByteArrayMatcherTest
{
    @Test
    public void metaTestForByteArrayMatcher()
    {
        byte[] a = new byte[] { 1, -2, 3 };
        byte[] b = new byte[] { 1, 3, -2 };
        assertThat( "a == a", a, byteArray( a ) );

        String caughtError = null;
        try
        {
            assertThat( "a != b", a, byteArray( b ) ); // this must fail
        }
        catch ( AssertionError error )
        {
            caughtError = error.getMessage();
        }
        String expectedMessage = "Expected: byte[] { 01 03 FE }";
        String butMessage = "     but: byte[] { 01 FE 03 }";
        assertThat( "should have thrown on a != b", caughtError, allOf(
                containsString( expectedMessage ),
                containsString( butMessage ) ) );
    }
}
