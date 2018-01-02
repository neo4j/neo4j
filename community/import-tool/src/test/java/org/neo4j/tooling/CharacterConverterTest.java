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
package org.neo4j.tooling;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CharacterConverterTest
{
    private final CharacterConverter converter = new CharacterConverter();

    @Test
    public void shouldConvertCharacter() throws Exception
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
    public void shouldConvertRawAscii() throws Exception
    {
        for ( char expected = 0; expected < Character.MAX_VALUE; expected++ )
        {
            assertCorrectConversion( expected, "\\" + (int) expected );
        }
    }

    @Test
    public void shouldConvertEscaped_t_AsTab() throws Exception
    {
        // GIVEN
        char expected = '\t';

        // THEN
        assertCorrectConversion( expected, "\\t" );
    }

    @Test
    public void shouldConvertSpelledOut_TAB_AsTab() throws Exception
    {
        // GIVEN
        char expected = '\t';

        // THEN
        assertCorrectConversion( expected, "TAB" );
    }

    @Test
    public void shouldNotAcceptRandomEscapedStrings() throws Exception
    {
        try
        {
            converter.apply( "\\bogus" );
            fail( "Should fail" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    @Test
    public void shouldNotAcceptStrings() throws Exception
    {
        try
        {
            converter.apply( "bogus" );
            fail( "Should fail" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    private void assertCorrectConversion( char expected, String material )
    {
        // WHEN
        char converted = converter.apply( material );

        // THEN
        assertEquals( expected, converted );
    }
}
