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
import static org.junit.Assert.fail;

public class ExtractorsTest
{
    @Test
    public void shouldExtractStringArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String data = "abcde,fghijkl,mnopq";

        // WHEN
        String[] values = (String[]) extractors.valueOf( "STRING[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( new String[] {"abcde","fghijkl","mnopq"}, values );
    }

    @Test
    public void shouldExtractLongArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        long[] longData = new long[] {123,4567,987654321};
        String data = toString( longData, ',' );

        // WHEN
        long[] values = (long[]) extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( longData, values );
    }

    @Test
    public void shouldExtractBooleanArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        boolean[] booleanData = new boolean[] {true, false, true};
        String data = toString( booleanData, ',' );

        // WHEN
        boolean[] values = (boolean[]) extractors.valueOf( "boolean[]" ).extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertBooleanArrayEquals( booleanData, values );
    }

    @Test
    public void shouldExtractDoubleArray() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        double[] doubleData = new double[] {123.123,4567.4567,987654321.0987};
        String data = toString( doubleData, ',' );

        // WHEN
        double[] values = extractors.doubleArray().extract( data.toCharArray(), 0, data.length() );

        // THEN
        assertArrayEquals( doubleData, values, 0.001 );
    }

    @Test
    public void shouldFailExtractingLongArrayWhereAnyValueIsEmpty() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );
        long[] longData = new long[] {112233,4455,66778899};
        String data = toString( longData, ';' ) + ";";

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        try
        {
            extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length() );
            fail( "Should have failed" );
        }
        catch ( NumberFormatException e )
        {   // Great
        }
    }

    @Test
    public void shouldFailExtractingLongArrayWhereAnyValueIsntReallyANumber() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        String data = "123;456;abc;789";
        try
        {
            extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length() );
            fail( "Should have failed" );
        }
        catch ( NumberFormatException e )
        {   // Great
        }
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

    @Test
    public void shouldExtractEmptyStringForEmptyArrayString() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = "";

        // WHEN
        String[] extracted = extractors.stringArray().extract( value.toCharArray(), 0, value.length() );

        // THEN
        assertEquals( 0, extracted.length );
    }

    @Test
    public void shouldExtractEmptyLongArrayForEmptyArrayString() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = "";

        // WHEN
        long[] extracted = extractors.longArray().extract( value.toCharArray(), 0, value.length() );

        // THEN
        assertEquals( 0, extracted.length );
    }

    @Test
    public void shouldExtractTwoEmptyStringsForSingleDelimiterInArrayString() throws Exception
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = ",";

        // WHEN
        String[] extracted = extractors.stringArray().extract( value.toCharArray(), 0, value.length() );

        // THEN
        assertArrayEquals( new String[] { "", "" }, extracted );
    }

    private String toString( long[] values, char delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( long value : values )
        {
            builder.append( builder.length() > 0 ? delimiter : "" ).append( value );
        }
        return builder.toString();
    }

    private String toString( double[] values, char delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( double value : values )
        {
            builder.append( builder.length() > 0 ? delimiter : "" ).append( value );
        }
        return builder.toString();
    }

    private String toString( boolean[] values, char delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( boolean value : values )
        {
            builder.append( builder.length() > 0 ? delimiter : "" ).append( value );
        }
        return builder.toString();
    }

    private void assertBooleanArrayEquals( boolean[] expected, boolean[] values )
    {
        assertEquals( "Array lengths differ", expected.length, values.length );
        for ( int i = 0; i < expected.length; i++ )
        {
            assertEquals( "Item " + i + " differs", expected[i], values[i] );
        }
    }
}
