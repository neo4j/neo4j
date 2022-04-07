/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShellStatementParserTest
{
    private final ShellStatementParser parser = new ShellStatementParser();

    @Test
    void parseEmptyLineShouldStartNewRow()
    {
        // when
        parser.parseMoreText( "" );

        // then
        assertFalse( parser.containsText() );
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();
        assertEquals( 1, statements.size() );
        assertEquals( ";", statements.get( 0 ) );
    }

    @Test
    void parseNewLineDoesNothing()
    {
        // when
        parser.parseMoreText( "\n" );

        // then
        assertFalse( parser.containsText() );
        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void parseAShellCommand()
    {
        // when
        parser.parseMoreText( "  :help exit bob snob  " );

        // then
        assertFalse( parser.containsText() );
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "  :help exit bob snob  ", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void parseAShellCommandWithNewLine()
    {
        // when
        parser.parseMoreText( ":help exit bob snob\n" );

        // then
        assertFalse( parser.containsText() );
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( ":help exit bob snob\n", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void parseIncompleteCypher()
    {
        // when
        parser.parseMoreText( "CREATE ()\n" );

        // then
        assertTrue( parser.containsText() );
        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void parseCompleteCypher()
    {
        // when
        parser.parseMoreText( "CREATE (n)\n" );
        assertTrue( parser.containsText() );
        parser.parseMoreText( "CREATE ();" );
        assertFalse( parser.containsText() );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "CREATE (n)\nCREATE ();", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void parseMultipleCypherSingleLine()
    {
        // when
        parser.parseMoreText( "RETURN 1;RETURN 2;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 2, statements.size() );
        assertEquals( "RETURN 1;", statements.get( 0 ) );
        assertEquals( "RETURN 2;", statements.get( 1 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void parseMultipleCypherMultipleLine()
    {
        // when
        parser.parseMoreText( "RETURN 1;" );
        parser.parseMoreText( "RETURN 2;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 2, statements.size() );
        assertEquals( "RETURN 1;", statements.get( 0 ) );
        assertEquals( "RETURN 2;", statements.get( 1 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void singleQuotedSemicolon()
    {
        // when
        parser.parseMoreText( "hello '\n" );
        parser.parseMoreText( ";\n" );
        parser.parseMoreText( "'\n" );
        parser.parseMoreText( ";\n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "hello '\n;\n'\n;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void backtickQuotedSemicolon()
    {
        // when
        parser.parseMoreText( "hello `\n" );
        parser.parseMoreText( ";\n" );
        parser.parseMoreText( "`\n" );
        parser.parseMoreText( ";  \n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "hello `\n;\n`\n;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void doubleQuotedSemicolon()
    {
        // when
        parser.parseMoreText( "hello \"\n" );
        parser.parseMoreText( ";\n" );
        parser.parseMoreText( "\"\n" );
        parser.parseMoreText( ";   \n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "hello \"\n;\n\"\n;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void escapedChars()
    {
        // when
        parser.parseMoreText( "one \\;\n" );
        parser.parseMoreText( "\"two \\\"\n" );
        parser.parseMoreText( ";\n" );
        parser.parseMoreText( "\";\n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "one \\;\n\"two \\\"\n;\n\";", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void nestedQuoting()
    {
        // when
        parser.parseMoreText( "go `tick;'single;\"double;\n" );
        parser.parseMoreText( "end`;\n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "go `tick;'single;\"double;\nend`;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void mixCommandAndCypherWithSpacingsAdded()
    {
        // when
        parser.parseMoreText( " :help me \n" );
        parser.parseMoreText( " cypher me up \n" );
        parser.parseMoreText( " :scotty \n" );
        parser.parseMoreText( " ; \n" );
        parser.parseMoreText( " :do it now! \n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 3, statements.size() );
        assertEquals( " :help me \n", statements.get( 0 ) );
        assertEquals( "cypher me up \n :scotty \n ;", statements.get( 1 ) );
        assertEquals( " :do it now! \n", statements.get( 2 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void commentHandlingIfSemicolon()
    {
        // when
        parser.parseMoreText( " first // ;\n" );
        parser.parseMoreText( "// /* ;\n" );
        parser.parseMoreText( " third ; // actually a semicolon here\n" );

        // then
        assertTrue( parser.hasStatements() );
        assertFalse( parser.containsText() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "first  third ;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
    }

    @Test
    void backslashDeadInBlockQuote()
    {
        // when
        parser.parseMoreText( "/* block \\*/\nCREATE ();" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "CREATE ();", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void commentInQuote()
    {
        // when
        parser.parseMoreText( "` here // comment `;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "` here // comment `;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void blockCommentInQuote()
    {
        // when
        parser.parseMoreText( "` here /* comment `;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( "` here /* comment `;", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void quoteInComment()
    {
        // when
        parser.parseMoreText( "// `;\n;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( ";", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void quoteInBlockomment()
    {
        // when
        parser.parseMoreText( "/* `;\n;*/\n;" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 1, statements.size() );
        assertEquals( ";", statements.get( 0 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void testReset()
    {
        // given
        parser.parseMoreText( "/* `;\n;*/\n;" );
        parser.parseMoreText( "bob" );
        assertTrue( parser.hasStatements() );
        assertTrue( parser.containsText() );

        // when
        parser.reset();

        // then
        assertFalse( parser.hasStatements() );
        assertFalse( parser.containsText() );
    }

    @Test
    void commentsBeforeBegin()
    {
        // when
        parser.parseMoreText( "//comment \n" );
        parser.parseMoreText( ":begin\n" );
        parser.parseMoreText( "RETURN 42;\n" );
        parser.parseMoreText( ":end\n" );

        // then
        assertTrue( parser.hasStatements() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 3, statements.size() );
        assertEquals( ":begin\n", statements.get( 0 ) );
        assertEquals( "RETURN 42;", statements.get( 1 ) );
        assertEquals( ":end\n", statements.get( 2 ) );

        assertFalse( parser.hasStatements() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void trimWhiteSpace()
    {
        // when
        parser.parseMoreText( "\t \r\n match (n) return n;\n\t    return 3;\t\r\n " );

        // then
        assertTrue( parser.hasStatements() );
        assertTrue( parser.incompleteStatement().isEmpty() );

        List<String> statements = parser.consumeStatements();

        assertEquals( 2, statements.size() );
        assertEquals( "match (n) return n;", statements.get( 0 ) );
        assertEquals( "return 3;", statements.get( 1 ) );

        assertFalse( parser.hasStatements() );
        assertTrue( parser.incompleteStatement().isEmpty() );
        assertEquals( 0, parser.consumeStatements().size() );
        assertFalse( parser.containsText() );
    }

    @Test
    void parserReset()
    {
        parser.parseMoreText( "match (n) // Hi\nreturn n; // Hi again" );

        assertTrue( parser.hasStatements() );
        List<String> statements = parser.consumeStatements();
        assertEquals( 1, statements.size() );
        assertEquals( "match (n) return n;", statements.get( 0 ) );

        parser.reset();

        parser.parseMoreText( "match (n) // Hi\nreturn n; // Hi again" );

        assertTrue( parser.hasStatements() );
        statements = parser.consumeStatements();
        assertEquals( 1, statements.size() );
        assertEquals( "match (n) return n;", statements.get( 0 ) );
    }
}
