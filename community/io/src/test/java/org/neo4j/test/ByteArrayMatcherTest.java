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
package org.neo4j.test;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.test.ByteArrayMatcher.byteArray;

public class ByteArrayMatcherTest
{
    @Test
    public void metaTestForByteArrayMatcher()
    {
        byte[] a = new byte[] { 1, -2, 3 };
        byte[] b = new byte[] { 1, 3, -2 };
        assertThat( "a == a", a, byteArray( a ) );

        boolean caughtError = false;
        try
        {
            assertThat( "a != b", a, byteArray( b ) ); // this must fail
        } catch ( AssertionError error )
        {
            caughtError = true;
        }
        assertTrue( "should have thrown on a != b", caughtError );
    }
}
