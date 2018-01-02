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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class ByteArrayMatcher extends TypeSafeDiagnosingMatcher<byte[]>
{
    public static ByteArrayMatcher byteArray( byte[] expected )
    {
        return new ByteArrayMatcher( expected );
    }

    private final byte[] expected;

    public ByteArrayMatcher( byte[] expected )
    {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely( byte[] actual, Description description )
    {
        describe( actual, description );
        if ( actual.length != expected.length )
        {
            return false;
        }
        for ( int i = 0; i < expected.length; i++ )
        {
            if ( actual[i] != expected[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void describeTo( Description description )
    {
        describe( expected, description );
    }

    private void describe( byte[] bytes, Description description )
    {
        description.appendText( "byte[] { " );
        for ( int i = 0; i < bytes.length; i++ )
        {
            int b = bytes[i] & 0xFF;
            description.appendText( String.format( "%02X ", b ) );
        }
        description.appendText( "}" );
    }
}
