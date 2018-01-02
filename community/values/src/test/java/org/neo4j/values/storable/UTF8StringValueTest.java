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
package org.neo4j.values.storable;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;

public class UTF8StringValueTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private String[] strings = {"", "1337", " ", "普通话/普通話", "\uD83D\uDE21", " a b c ", "䤹᳽", "熨", "ۼ",
            "ⲹ楡톜ഷۢ⼈늉₭샺ጚ砧攡跿家䯶鲏⬖돛犽ۼ",
            " 㺂࿝鋦毠",//first character is a thin space,
            "\u0018", ";먵熍裬岰鷲趫\uA8C5얱㓙髿ᚳᬼ≩萌 ", "\u001cӳ"
    };

    @Test
    public void shouldHandleDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertEqual( stringValue, utf8 );
            assertThat( stringValue.length(), equalTo( utf8.length() ) );
        }
    }

    @Test
    public void shouldTrimDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.trim(), utf8.trim() );
        }
    }

    @Test
    public void shouldLTrimDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.ltrim(), utf8.ltrim() );
        }
    }

    @Test
    public void trimShouldBeSameAsLtrimAndRtrim()
    {
        for ( String string : strings )
        {
            TextValue utf8 = utf8Value( string.getBytes( UTF_8 ) );
            assertSame( utf8.trim(), utf8.ltrim().rtrim() );
        }
    }

    @Test
    public void shouldSubstring()
    {
        String string = "ü";
        TextValue utf8 = utf8Value( string.getBytes( UTF_8 ) );
        assertThat( utf8.substring( 0, 1 ).stringValue(), equalTo( "ü" ) );
    }

    @Test
    public void shouldRTrimDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.rtrim(), utf8.rtrim() );
        }
    }

    @Test
    public void shouldCompareTo()
    {
        for ( String string1 : strings )
        {
            for ( String string2 : strings )
            {

                int x = stringValue( string1 ).compareTo( utf8Value( string2.getBytes( UTF_8 ) ) );
                int y = utf8Value( string1.getBytes( UTF_8 ) ).compareTo( stringValue( string2 ) );
                int z = utf8Value( string1.getBytes( UTF_8 ) )
                        .compareTo( utf8Value( string2.getBytes( UTF_8 ) ) );

                assertThat( x, equalTo( y ) );
                assertThat( x, equalTo( z ) );
            }
        }
    }

    @Test
    public void shouldReverse()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue.reverse(), utf8.reverse() );
        }
    }

    @Test
    public void shouldHandleOffset()
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

    private void assertSame( TextValue lhs, TextValue rhs )
    {
        assertThat( format( "%s.length != %s.length", lhs, rhs ), lhs.length(),
                equalTo( rhs.length() ) );
        assertThat( format( "%s != %s", lhs, rhs ), lhs, equalTo( rhs ) );
        assertThat( format( "%s != %s", rhs, lhs ), rhs, equalTo( lhs ) );
        assertThat( format( "%s.hashCode != %s.hashCode", rhs, lhs ), lhs.hashCode(), equalTo( rhs.hashCode() ) );
        assertThat( lhs, equalTo( rhs ) );
    }

    @Test
    public void shouldHandleTooLargeStartPointInSubstring()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // When
        TextValue substring = value.substring( 8, 5 );

        // Then
        assertThat( substring, equalTo( StringValue.EMTPY ) );
    }

    @Test
    public void shouldHandleTooLargeLengthInSubstring()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // When
        TextValue substring = value.substring( 3, 76 );

        // Then
        assertThat( substring.stringValue(), equalTo( "lo" ) );
    }

    @Test
    public void shouldThrowOnNegativeStart()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // Expect
        exception.expect( IndexOutOfBoundsException.class );

        // When
        value.substring( -4, 3 );
    }

    @Test
    public void shouldThrowOnNegativeLength()
    {
        // Given
        TextValue value = utf8Value( "hello".getBytes( UTF_8 ) );

        // Expect
        exception.expect( IndexOutOfBoundsException.class );

        // When
        value.substring( 4, -3 );
    }
}
