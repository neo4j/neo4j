/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class UTF8StringValueBuilderTest
{

    @Test
    void shouldHandleSingleByteCodePoints()
    {
        // Given
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder();
        int codepoint = "$".codePointAt( 0 );

        // When
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );

        // Then
        TextValue textValue = builder.build();
        assertThat( textValue.stringValue(), equalTo("$$$"));
    }

    @Test
    void shouldHandleTwoByteCodePoints()
    {
        // Given
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder();
        int codepoint = "¢".codePointAt( 0 );

        // When
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );

        // Then
        TextValue textValue = builder.build();
        assertThat( textValue.stringValue(), equalTo("¢¢¢"));
    }

    @Test
    void shouldHandleThreeByteCodePoints()
    {
        // Given
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder();
        int codepoint = "€".codePointAt( 0 );

        // When
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );

        // Then
        TextValue textValue = builder.build();
        assertThat( textValue.stringValue(), equalTo("€€€"));
    }

    @Test
    void shouldHandleFourByteCodePoints()
    {
        // Given
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder();
        int codepoint = "\uD800\uDF48".codePointAt( 0 );

        // When
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );
        builder.addCodePoint( codepoint );

        // Then
        TextValue textValue = builder.build();
        assertThat( textValue.stringValue(), equalTo("\uD800\uDF48\uD800\uDF48\uD800\uDF48"));
    }
}
