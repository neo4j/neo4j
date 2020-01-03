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
package org.neo4j.tooling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CharacterConverterTest
{
    private final CharacterConverter converter = new CharacterConverter();

    @Test
    void shouldConvertCharacter()
    {
        // GIVEN
        String candidates = "abcdefghijklmnopqrstuvwxyzåäö\"'^`\\"; // to name a few

        // THEN
        for ( int i = 0; i < candidates.length(); i++ )
        {
            char expected = candidates.charAt( i );
            assertCorrectConversion( expected, String.valueOf( expected ) );
        }
    }

    @Test
    void shouldConvertRawAscii()
    {
        for ( char expected = 0; expected < Character.MAX_VALUE; expected++ )
        {
            assertCorrectConversion( expected, "\\" + (int) expected );
        }
    }

    @Test
    void shouldConvertEscaped_t_AsTab()
    {
        // GIVEN
        char expected = '\t';

        // THEN
        assertCorrectConversion( expected, "\\t" );
    }

    @Test
    void shouldConvert_t_AsTab()
    {
        // GIVEN
        char expected = '\t';

        // THEN
        assertCorrectConversion( expected, "\t" );
    }

    @Test
    void shouldConvertSpelledOut_TAB_AsTab()
    {
        // GIVEN
        char expected = '\t';

        // THEN
        assertCorrectConversion( expected, "TAB" );
    }

    @Test
    void shouldNotAcceptRandomEscapedStrings()
    {
        assertThrows( IllegalArgumentException.class, () -> converter.apply( "\\bogus" ) );
    }

    @Test
    void shouldNotAcceptStrings()
    {
        assertThrows( IllegalArgumentException.class, () -> converter.apply( "bogus" ) );
    }

    private void assertCorrectConversion( char expected, String material )
    {
        // WHEN
        char converted = converter.apply( material );

        // THEN
        assertEquals( expected, converted );
    }
}
