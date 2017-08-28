/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

public class UTF8StringValueTest
{
    private String[] strings = {"", "1337", " ", "普通话/普通話", "\uD83D\uDE21"};

    @Test
    public void shouldHandleDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( StandardCharsets.UTF_8 );
            TextValue utf8 = utf8Value( bytes );
            assertSame( stringValue, utf8 );
        }
    }

    @Test
    public void shouldHandleOffset()
    {
        // Given
        byte[] bytes = "abcdefg".getBytes( StandardCharsets.UTF_8 );

        // When
        TextValue textValue = utf8Value( bytes, 3, 2 );

        // Then
        assertSame( textValue, stringValue( "de" ) );
    }

    private void assertSame( TextValue lhs, TextValue rhs )
    {
        assertThat( lhs.length(), equalTo( rhs.length() ) );
        assertThat( lhs, equalTo( rhs ) );
        assertThat( rhs, equalTo( lhs ) );
        assertThat( lhs.hashCode(), equalTo( rhs.hashCode() ) );
    }
}
