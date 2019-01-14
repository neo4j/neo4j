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

import org.junit.jupiter.api.Test;


import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.StringsLibrary.STRINGS;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;

class UTF8StringValueTest
{
    @Test
    void shouldHandleDifferentTypesOfStrings()
    {
        for ( String string : STRINGS )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertEqual( stringValue, utf8 );
            assertThat( stringValue.length(), equalTo( utf8.length() ) );
        }
    }

    @Test
    void shouldTrimDifferentTypesOfStrings()
    {
        for ( String string : STRINGS )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.trim(), utf8.trim() );
        }
    }

    @Test
    void shouldLTrimDifferentTypesOfStrings()
    {
        for ( String string : STRINGS )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.ltrim(), utf8.ltrim() );
        }
    }

    @Test
    void trimShouldBeSameAsLtrimAndRtrim()
    {
        for ( String string : STRINGS )
        {
            TextValue utf8 = utf8Value( string.getBytes( UTF_8 ) );
            assertSame( utf8.trim(), utf8.ltrim().rtrim() );
        }
    }

    @Test
    void shouldSubstring()
    {
        String string = "ü";
        TextValue utf8 = utf8Value( string.getBytes( UTF_8 ) );
        assertThat( utf8.substring( 0, 1 ).stringValue(), equalTo( "ü" ) );
    }

    @Test
    void shouldRTrimDifferentTypesOfStrings()
    {
        for ( String string : STRINGS )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.rtrim(), utf8.rtrim() );
        }
    }

    @Test
    void shouldCompareTo()
    {
        for ( String string1 : STRINGS )
        {
            for ( String string2 : STRINGS )
            {
                assertCompareTo( string1, string2 );
            }
        }
    }

    static void assertCompareTo( String string1, String string2 )
    {
        TextValue textValue1 = stringValue( string1 );
        TextValue textValue2 = stringValue( string2 );
        TextValue utf8Value1 = utf8Value( string1.getBytes( UTF_8 ) );
        TextValue utf8Value2 = utf8Value( string2.getBytes( UTF_8 ) );
        int a = textValue1.compareTo( textValue2 );
        int x = textValue1.compareTo( utf8Value2 );
        int y = utf8Value1.compareTo( textValue2 );
        int z = utf8Value1.compareTo( utf8Value2 );

        assertThat( Math.signum( a ), equalTo( Math.signum( x ) ) );
        assertThat( Math.signum( a ), equalTo( Math.signum( y ) ) );
        assertThat( Math.signum( a ), equalTo( Math.signum( z ) ) );
    }

    @Test
    void shouldReverse()
    {
        for ( String string : STRINGS )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.reverse(), utf8.reverse() );
        }
    }

    @Test
    void shouldHandleOffset()
    {
        // Given
        byte[] bytes = "abcdefg".getBytes( UTF_8 );

        // When
        TextValue textValue = utf8Value( bytes, 3, 2 );

        // Then
        assertSame( textValue, stringValue( "de" ) );
        assertThat( textValue.length(), equalTo( stringValue( "de" ).length() ) );
        assertSame( textValue.reverse(), stringValue( "ed" ) );
    }

    @Test
    void shouldHandleAdditionWithOffset()
    {
        // Given
        byte[] bytes = "abcdefg".getBytes( UTF_8 );

        // When
        UTF8StringValue a = (UTF8StringValue) utf8Value( bytes, 1, 2 );
        UTF8StringValue b = (UTF8StringValue) utf8Value( bytes, 3, 3 );

        // Then
       assertSame( a.plus( a ),  stringValue( "bcbc" ) );
       assertSame( a.plus( b ), stringValue( "bcdef" ) );
       assertSame( b.plus( a ), stringValue( "defbc" ) );
       assertSame( b.plus( b ), stringValue( "defdef" ) );
    }

    @Test
    void shouldHandleAdditionWithOffsetAndNonAscii()
    {
        // Given, two characters that require three bytes each
        byte[] bytes = "ⲹ楡".getBytes( UTF_8 );

        // When
        UTF8StringValue a = (UTF8StringValue) utf8Value( bytes, 0, 3 );
        UTF8StringValue b = (UTF8StringValue) utf8Value( bytes, 3, 3 );

        // Then
        assertSame( a.plus( a ), stringValue(  "ⲹⲹ" ) );
        assertSame( a.plus( b ), stringValue(  "ⲹ楡" ) );
        assertSame( b.plus( a ), stringValue(  "楡ⲹ") );
        assertSame( b.plus( b ), stringValue( "楡楡" ) );
    }

    private void assertSame( TextValue lhs, TextValue rhs )
    {
        assertThat( format( "%s.length != %s.length", lhs, rhs ), lhs.length(),
                equalTo( rhs.length() ) );
        assertThat( format( "%s != %s", lhs, rhs ), lhs, equalTo( rhs ) );
        assertThat( format( "%s != %s", rhs, lhs ), rhs, equalTo( lhs ) );
        assertThat( format( "%s.hashCode != %s.hashCode", rhs, lhs ), lhs.hashCode(), equalTo( rhs.hashCode() ) );
        assertThat( format( "%s.hashCode64 != %s.hashCode64", rhs, lhs ),
                lhs.hashCode64(), equalTo( rhs.hashCode64() ) );
        assertThat( lhs, equalTo( rhs ) );
    }

    @Test
    void shouldHandleTooLargeStartPointInSubstring()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // When
        TextValue substring = value.substring( 8, 5 );

        // Then
        assertThat( substring, equalTo( StringValue.EMPTY ) );
    }

    @Test
    void shouldHandleTooLargeLengthInSubstring()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // When
        TextValue substring = value.substring( 3, 76 );

        // Then
        assertThat( substring.stringValue(), equalTo( "lo" ) );
    }

    @Test
    void shouldThrowOnNegativeStart()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        assertThrows( IndexOutOfBoundsException.class, () -> value.substring( -4, 3 ) );
    }

    @Test
    void shouldThrowOnNegativeLength()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        assertThrows( IndexOutOfBoundsException.class, () -> value.substring( 4, -3 ) );
    }

    @Test
    void shouldHandleStringPredicatesWithOffset()
    {
        // Given
        byte[] bytes = "abcdefghijklmnoprstuvxyzABCDEFGHIJKLMNOPRSTUVXYZ".getBytes( UTF_8 );

        for ( int offset = 0; offset <= bytes.length; offset++ )
        {
            for ( int length = 0; length < bytes.length - offset; length++ )
            {
                TextValue value = utf8Value( bytes, offset, length );

                for ( int otherOffset = 0; otherOffset <= bytes.length; otherOffset++ )
                {
                    for ( int otherLength = 0; otherLength < bytes.length - otherOffset; otherLength++ )
                    {
                        TextValue other = utf8Value( bytes, otherOffset, otherLength );
                        assertThat( value.startsWith( other ),
                                equalTo( otherLength == 0 || otherOffset == offset && otherLength <= length ) );
                        assertThat( value.endsWith( other ),
                                equalTo( otherLength == 0 ||
                                         otherOffset >= offset && otherLength == length + offset - otherOffset ) );
                        assertThat( value.contains( other ),
                                equalTo( otherLength == 0 ||
                                         otherOffset >= offset && otherLength <= length + offset - otherOffset ) );

                    }
                }

            }
        }
    }
}
