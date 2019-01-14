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
package org.neo4j.test.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class ByteArrayMatcher extends TypeSafeDiagnosingMatcher<byte[]>
{
    private static final char[] hexadecimals =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static ByteArrayMatcher byteArray( byte... expected )
    {
        return new ByteArrayMatcher( expected );
    }

    public static ByteArrayMatcher byteArray( int... expected )
    {
        byte[] bytes = new byte[expected.length];
        for ( int i = 0; i < expected.length; i++ )
        {
            bytes[i] = (byte) expected[i];
        }
        return byteArray( bytes );
    }

    private final byte[] expected;

    public ByteArrayMatcher( byte[] expected )
    {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely( byte[] actual, Description description )
    {
        if ( actual.length != expected.length )
        {
            describe( actual, description );
            return false;
        }
        for ( int i = 0; i < expected.length; i++ )
        {
            if ( actual[i] != expected[i] )
            {
                describe( actual, description );
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
        String prefix = "byte[] { ";
        String suffix = "}";
        StringBuilder sb = new StringBuilder( bytes.length * 3 + prefix.length() + suffix.length() );

        sb.append( prefix );
        //noinspection ForLoopReplaceableByForEach
        for ( int i = 0; i < bytes.length; i++ )
        {
            int b = bytes[i] & 0xFF;
            char hi = hexadecimals[b >> 4];
            char lo = hexadecimals[b & 0x0F];
            sb.append( hi ).append( lo ).append( ' ' );
        }
        sb.append( suffix );
        description.appendText( sb.toString() );
    }
}
