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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.logging.internal.LogMessageUtil.slf4jToStringFormatPlaceholders;

class LogMessageUtilTest
{
    @Test
    void shouldThrowWhenStringIsNull()
    {
        assertThrows( NullPointerException.class, () -> slf4jToStringFormatPlaceholders( null ) );
    }

    @Test
    void shouldDoNothingForEmptyString()
    {
        assertEquals( "", slf4jToStringFormatPlaceholders( "" ) );
    }

    @Test
    void shouldDoNothingForStringWithoutPlaceholders()
    {
        assertEquals( "Simple log message", slf4jToStringFormatPlaceholders( "Simple log message" ) );
    }

    @Test
    void shouldReplaceSlf4jPlaceholderWithStringFormatPlaceholder()
    {
        assertEquals( "Log message with %s single placeholder", slf4jToStringFormatPlaceholders( "Log message with {} single placeholder" ) );
        assertEquals( "Log message %s with two %s placeholders", slf4jToStringFormatPlaceholders( "Log message {} with two {} placeholders" ) );
        assertEquals( "Log %s message %s with three %s placeholders", slf4jToStringFormatPlaceholders( "Log {} message {} with three {} placeholders" ) );
    }
}
