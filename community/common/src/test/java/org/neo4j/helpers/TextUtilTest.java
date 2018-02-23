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
package org.neo4j.helpers;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TextUtilTest
{
    @Test
    public void shouldReplaceVariablesWithValuesInTemplateString()
    {
        // given
        String template = "This is a $FIRST that $SECOND $THIRD!";
        Map<String,String> values = new HashMap<>();
        values.put( "FIRST", "String" );
        values.put( "SECOND", "should" );
        values.put( "THIRD", "act as a template!" );

        // when
        String string = TextUtil.templateString( template, values );

        // then
        assertEquals( "This is a String that should act as a template!!", string );
    }

    @Test
    public void shouldTokenizeStringWithWithoutQuotes()
    {
        // given
        String untokenized = "First Second Third";

        // when
        String[] tokenized = TextUtil.tokenizeStringWithQuotes( untokenized );

        // then
        assertArrayEquals( new String[] {"First", "Second", "Third"}, tokenized );
    }

    @Test
    public void shouldTokenizeStringWithQuotes()
    {
        // given
        String untokenized = "First \"Second one\" Third \"And a fourth\"";

        // when
        String[] tokenized = TextUtil.tokenizeStringWithQuotes( untokenized );

        // then
        assertArrayEquals( new String[] {"First", "Second one", "Third", "And a fourth"}, tokenized );
    }

    @Test
    public void shouldTokenStringWithWithQuotesAndEscapedSpaces()
    {
        // given
        String untokenized = "First \"Second one\" Third And\\ a\\ fourth";

        // when
        String[] tokenized = TextUtil.tokenizeStringWithQuotes( untokenized );

        // then
        assertArrayEquals( new String[] {"First", "Second one", "Third", "And a fourth"}, tokenized );
    }

    @Test
    public void shouldPreserveBackslashes()
    {
        // given
        String untokenized = "First C:\\a\\b\\c";

        // when
        String[] tokenized = TextUtil.tokenizeStringWithQuotes( untokenized, true, true );

        // then
        assertArrayEquals( new String[] {"First", "C:\\a\\b\\c"}, tokenized );
    }
}
