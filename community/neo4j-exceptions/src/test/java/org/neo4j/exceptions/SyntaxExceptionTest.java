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
package org.neo4j.exceptions;

import org.junit.jupiter.api.Test;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SyntaxExceptionTest
{

    @Test
    void messageWithoutOffset()
    {
        String message = "Message";
        SyntaxException e = new SyntaxException( message );
        assertEquals( message, e.getMessage() );
    }

    @Test
    void messageWithEmptyQuery()
    {
        String message = "Message";
        String query = "";
        int offset = 0;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"\"%s ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithJustNewlineQuery()
    {
        String message = "Message";
        String query = "\n";
        int offset = 0;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"\"%s ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithSingleLineQuery()
    {
        String message = "Message";
        String query = "The error is here.";
        int offset = 13;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s              ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithSingleLineQueryAndOffsetTooHigh()
    {
        String message = "Message";
        String query = "The error is here.";
        int offset = 100;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s                   ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithMultiLineQueryAndOffsetInFirstLine()
    {
        String message = "Message";
        String query = "The error is here.\nSome random text.";
        int offset = 13;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s              ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithMultiLineQueryAndOffsetAtEndOfFirstLine()
    {
        String message = "Message";
        String query = "The error is here.\nSome random text.";
        int offset = 18;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s                   ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithMultiLineQueryAndOffsetAtBeginningOfSecondLine()
    {
        String message = "Message";
        String query = "The error is here.\nSome random text.";
        int offset = 19;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"Some random text.\"%s ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithMultiLineQueryAndOffsetInSecondLine()
    {
        String message = "Message";
        String query = "Some random text.\nThe error is here.";
        int offset = 31;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s              ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }

    @Test
    void messageWithMultiLineQueryAndOffsetTooHigh()
    {
        String message = "Message";
        String query = "Some random text.\nThe error is here.";
        int offset = 100;
        SyntaxException e = new SyntaxException( message, query, offset );

        String expected = String.format( "Message%s\"The error is here.\"%s                   ^", lineSeparator(), lineSeparator() );

        assertEquals( expected, e.getMessage() );
    }
}
