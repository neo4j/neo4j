/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.csv.reader;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ExtractorsTest
{
    @Test
    public void shouldExtractStringArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );

        // WHEN
        String data = "abcde,fghijkl,mnopq";
        String[] values = (String[]) extractors.valueOf( "STRING[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( new String[] {"abcde","fghijkl","mnopq"}, values );
    }

    @Test
    public void shouldExtractLongArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );

        // WHEN
        long[] longData = new long[] {123,4567,987654321};
        String data = toString( longData, ',' );
        long[] values = (long[]) extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( longData, values );
    }

    @Test
    public void shouldExtractLongArrayWithADelimiterLast() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );

        // WHEN
        long[] longData = new long[] {112233,4455,66778899};
        String data = toString( longData, ';' ) + ";";
        long[] values = (long[]) extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( longData, values );
    }

    @Test
    public void shouldExtractNegativeInt() throws Exception
    {
        // GIVEN
        int value = -1234567;

        // WHEN
        char[] asChars = String.valueOf( value ).toCharArray();
        int extracted = Extractors.INT.extract( asChars, 0, asChars.length );

        // THEN
        assertEquals( value, extracted );
    }

    private String toString( long[] longData, char delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( long value : longData )
        {
            builder.append( builder.length() > 0 ? delimiter : "" ).append( value );
        }
        return builder.toString();
    }
}
