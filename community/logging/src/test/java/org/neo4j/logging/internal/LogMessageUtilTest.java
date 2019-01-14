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
package org.neo4j.logging.internal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.logging.internal.LogMessageUtil.slf4jToStringFormatPlaceholders;

public class LogMessageUtilTest
{
    @Test
    public void shouldThrowWhenStringIsNull()
    {
        try
        {
            slf4jToStringFormatPlaceholders( null );
            fail( "Exception expected" );
        }
        catch ( NullPointerException ignore )
        {
        }
    }

    @Test
    public void shouldDoNothingForEmptyString()
    {
        assertEquals( "", slf4jToStringFormatPlaceholders( "" ) );
    }

    @Test
    public void shouldDoNothingForStringWithoutPlaceholders()
    {
        assertEquals( "Simple log message", slf4jToStringFormatPlaceholders( "Simple log message" ) );
    }

    @Test
    public void shouldReplaceSlf4jPlaceholderWithStringFormatPlaceholder()
    {
        assertEquals( "Log message with %s single placeholder", slf4jToStringFormatPlaceholders( "Log message with {} single placeholder" ) );
        assertEquals( "Log message %s with two %s placeholders", slf4jToStringFormatPlaceholders( "Log message {} with two {} placeholders" ) );
        assertEquals( "Log %s message %s with three %s placeholders", slf4jToStringFormatPlaceholders( "Log {} message {} with three {} placeholders" ) );
    }
}
