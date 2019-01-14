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
package org.neo4j.csv.reader;

import org.junit.Test;

import org.neo4j.csv.reader.Extractors.IntExtractor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ExtractorsTest
{
    @Test
    public void shouldExtractStringArray()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String data = "abcde,fghijkl,mnopq";

        // WHEN
        @SuppressWarnings( "unchecked" )
        Extractor<String[]> extractor = (Extractor<String[]>) extractors.valueOf( "STRING[]" );
        extractor.extract( data.toCharArray(), 0, data.length(), false );

        // THEN
        assertArrayEquals( new String[] {"abcde","fghijkl","mnopq"}, extractor.value() );
    }

    @Test
    public void shouldExtractLongArray()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        long[] longData = new long[] {123,4567,987654321};
        String data = toString( longData, ',' );

        // WHEN
        @SuppressWarnings( "unchecked" )
        Extractor<long[]> extractor = (Extractor<long[]>) extractors.valueOf( "long[]" );
        extractor.extract( data.toCharArray(), 0, data.length(), false );

        // THEN
        assertArrayEquals( longData, extractor.value() );
    }

    @Test
    public void shouldExtractBooleanArray()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        boolean[] booleanData = new boolean[] {true, false, true};
        String data = toString( booleanData, ',' );

        // WHEN
        Extractor<boolean[]> extractor = extractors.booleanArray();
        extractor.extract( data.toCharArray(), 0, data.length(), false );

        // THEN
        assertBooleanArrayEquals( booleanData, extractor.value() );
    }

    @Test
    public void shouldExtractDoubleArray()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        double[] doubleData = new double[] {123.123,4567.4567,987654321.0987};
        String data = toString( doubleData, ',' );

        // WHEN
        Extractor<double[]> extractor = extractors.doubleArray();
        extractor.extract( data.toCharArray(), 0, data.length(), false );

        // THEN
        assertArrayEquals( doubleData, extractor.value(), 0.001 );
    }

    @Test
    public void shouldFailExtractingLongArrayWhereAnyValueIsEmpty()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );
        long[] longData = new long[] {112233,4455,66778899};
        String data = toString( longData, ';' ) + ";";

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        try
        {
            extractors.longArray().extract( data.toCharArray(), 0, data.length(), false );
            fail( "Should have failed" );
        }
        catch ( NumberFormatException e )
        {   // Great
        }
    }

    @Test
    public void shouldFailExtractingLongArrayWhereAnyValueIsntReallyANumber()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        String data = "123;456;abc;789";
        try
        {
            extractors.valueOf( "long[]" ).extract( data.toCharArray(), 0, data.length(), false );
            fail( "Should have failed" );
        }
        catch ( NumberFormatException e )
        {   // Great
        }
    }

    @Test
    public void shouldExtractPoint()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        PointValue value = Values.pointValue( CoordinateReferenceSystem.WGS84, 13.2, 56.7 );

        // WHEN
        char[] asChars = "Point{latitude: 56.7, longitude: 13.2}".toCharArray();
        Extractors.PointExtractor extractor = extractors.point();
        String headerInfo = "{crs:WGS-84}";
        extractor.extract( asChars, 0, asChars.length, false, PointValue.parseHeaderInformation( headerInfo ) );

        // THEN
        assertEquals( value, extractor.value );
    }

    @Test
    public void shouldExtractNegativeInt()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        int value = -1234567;

        // WHEN
        char[] asChars = String.valueOf( value ).toCharArray();
        IntExtractor extractor = extractors.int_();
        extractor.extract( asChars, 0, asChars.length, false );

        // THEN
        assertEquals( value, extractor.intValue() );
    }

    @Test
    public void shouldExtractEmptyStringForEmptyArrayString()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = "";

        // WHEN
        Extractor<String[]> extractor = extractors.stringArray();
        extractor.extract( value.toCharArray(), 0, value.length(), false );

        // THEN
        assertEquals( 0, extractor.value().length );
    }

    @Test
    public void shouldExtractEmptyLongArrayForEmptyArrayString()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = "";

        // WHEN
        Extractor<long[]> extractor = extractors.longArray();
        extractor.extract( value.toCharArray(), 0, value.length(), false );

        // THEN
        assertEquals( 0, extractor.value().length );
    }

    @Test
    public void shouldExtractTwoEmptyStringsForSingleDelimiterInArrayString()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = ",";

        // WHEN
        Extractor<String[]> extractor = extractors.stringArray();
        extractor.extract( value.toCharArray(), 0, value.length(), false );

        // THEN
        assertArrayEquals( new String[] { "", "" }, extractor.value() );
    }

    @Test
    public void shouldExtractEmptyStringForEmptyQuotedString()
    {
        // GIVEN
        Extractors extractors = new Extractors( ',' );
        String value = "";

        // WHEN
        Extractor<String> extractor = extractors.string();
        extractor.extract( value.toCharArray(), 0, value.length(), true );

        // THEN
        assertEquals( "", extractor.value() );
    }

    @Test
    public void shouldExtractNullForEmptyQuotedStringIfConfiguredTo()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';', true );
        Extractor<String> extractor = extractors.string();

        // WHEN
        extractor.extract( new char[0], 0, 0, true );
        String extracted = extractor.value();

        // THEN
        assertNull( extracted );
    }

    @Test
    public void shouldTrimStringArrayIfConfiguredTo()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';', true, true );
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        extractor.extract( asChars, 0, asChars.length, true );

        // THEN
        assertArrayEquals( new String[] {"ab", "cd", "ef", "gh"}, extractor.value() );
    }

    @Test
    public void shouldNotTrimStringIfNotConfiguredTo()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';', true, false );
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        extractor.extract( asChars, 0, asChars.length, true );

        // THEN
        assertArrayEquals( new String[] {"ab", "cd ", " ef", " gh "}, extractor.value() );
    }

    @Test
    public void shouldCloneExtractor()
    {
        // GIVEN
        Extractors extractors = new Extractors( ';' );
        Extractor<String> e1 = extractors.string();
        Extractor<String> e2 = e1.clone();

        // WHEN
        String v1 = "abc";
        e1.extract( v1.toCharArray(), 0, v1.length(), false );
        assertEquals( v1, e1.value() );
        assertNull( e2.value() );

        // THEN
        String v2 = "def";
        e2.extract( v2.toCharArray(), 0, v2.length(), false );
        assertEquals( v2, e2.value() );
        assertEquals( v1, e1.value() );
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
