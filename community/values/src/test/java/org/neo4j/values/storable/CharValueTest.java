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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

public class CharValueTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private char[] chars = {' ', '楡', 'a', '7', 'Ö'};

    @Test
    public void shouldHandleDifferentTypesOfChars()
    {
        for ( char c : chars )
        {
            TextValue charValue = charValue( c );
            TextValue stringValue = stringValue( Character.toString( c ) );

            assertThat( charValue, equalTo( stringValue ) );
            assertThat( charValue.length(), equalTo( stringValue.length() ) );
            assertThat( charValue.hashCode(), equalTo( stringValue.hashCode() ) );
            assertThat( charValue.split( Character.toString( c ) ),
                    equalTo( stringValue.split( Character.toString( c ) ) ) );
            assertThat( charValue.toUpper(), equalTo( stringValue.toUpper() ) );
            assertThat( charValue.toLower(), equalTo( stringValue.toLower() ) );
        }
    }

    @Test
    public void shouldSplit()
    {
        CharValue charValue = charValue( 'a' );
        assertThat( charValue.split( "a" ), equalTo( list( EMPTY_STRING, EMPTY_STRING ) ) );
        assertThat( charValue.split( "A" ), equalTo( list( charValue ) ) );
    }

    @Test
    public void shouldTrim()
    {
        assertThat( charValue( 'a' ).trim(), equalTo( charValue( 'a' ) ) );
        assertThat( charValue( ' ' ).trim(), equalTo( EMPTY_STRING ) );
    }

    @Test
    public void shouldLTrim()
    {
        assertThat( charValue( 'a' ).ltrim(), equalTo( charValue( 'a' ) ) );
        assertThat( charValue( ' ' ).ltrim(), equalTo( EMPTY_STRING ) );
    }

    @Test
    public void shouldRTrim()
    {
        assertThat( charValue( 'a' ).rtrim(), equalTo( charValue( 'a' ) ) );
        assertThat( charValue( ' ' ).rtrim(), equalTo( EMPTY_STRING ) );
    }

    @Test
    public void shouldReverse()
    {
        for ( char c : chars )
        {
            CharValue charValue = charValue( c );
            assertThat( charValue.reverse(), equalTo( charValue ) );
        }
    }

    @Test
    public void shouldReplace()
    {
        assertThat( charValue( 'a' ).replace( "a", "a long string" ), equalTo( stringValue( "a long string" ) ) );
        assertThat( charValue( 'a' ).replace( "b", "a long string" ), equalTo( charValue( 'a' ) ) );
    }

    @Test
    public void shouldSubstring()
    {
        assertThat( charValue( 'a' ).substring( 0, 1 ), equalTo( charValue( 'a' ) ) );
        assertThat( charValue( 'a' ).substring( 1, 3 ), equalTo( EMPTY_STRING ) );
    }
}
