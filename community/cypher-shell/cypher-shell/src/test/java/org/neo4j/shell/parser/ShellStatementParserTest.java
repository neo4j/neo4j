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

import java.io.IOException;
import java.util.List;

import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.IncompleteStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ShellStatementParserTest
{
    private final ShellStatementParser parser = new ShellStatementParser();

    @Test
    void parseEmptyLineDoesNothing() throws IOException
    {
        assertStatements( "" );
    }

    @Test
    void parseNewLineDoesNothing() throws IOException
    {
        assertStatements( "\n" );
    }

    @Test
    void parseAShellCommand() throws IOException
    {
        var expected = new CommandStatement( ":help", List.of( "exit", "bob", "snob" ) );
        assertStatements( "  :help exit bob snob  ", expected );
    }

    @Test
    void parseAShellCommandWithNewLine() throws IOException
    {
        var expected = new CommandStatement( ":help", List.of( "exit", "bob", "snob" ) );
        assertStatements( "  :help exit bob snob  \n  ", expected );
    }

    @Test
    void parseIncompleteCypher() throws IOException
    {
        assertStatements( "CREATE ()\n", new IncompleteStatement( "CREATE ()\n" ) );
    }

    @Test
    void parseCompleteCypher() throws IOException
    {
        assertStatements( "CREATE (n)\nCREATE ();", new CypherStatement( "CREATE (n)\nCREATE ();" ) );
    }

    @Test
    void parseMultipleCypherSingleLine() throws IOException
    {
        assertStatements(
                "RETURN 1;RETURN 2;",
                new CypherStatement( "RETURN 1;" ),
                new CypherStatement( "RETURN 2;" )
        );
    }

    @Test
    void parseMultipleCypherMultipleLine() throws IOException
    {
        assertStatements(
                "RETURN 1;\n RETURN 2;",
                new CypherStatement( "RETURN 1;" ),
                new CypherStatement( "RETURN 2;" )
        );
    }

    @Test
    void singleQuotedSemicolon() throws IOException
    {
        assertStatements( "hello '\n;\n'\n;\n", new CypherStatement( "hello '\n;\n'\n;" ) );
    }

    @Test
    void backtickQuotedSemicolon() throws IOException
    {
        assertStatements( "hello `\n;\n`\n;  \n", new CypherStatement( "hello `\n;\n`\n;" ) );
    }

    @Test
    void doubleQuotedSemicolon() throws IOException
    {
        assertStatements( "hello \"\n;\n\"\n;   \n", new CypherStatement( "hello \"\n;\n\"\n;" ) );
    }

    @Test
    void escapedChars() throws IOException
    {
        assertStatements( "one \\;\n\"two \\\"\n;\n\";\n", new CypherStatement( "one \\;\n\"two \\\"\n;\n\";" ) );
    }

    @Test
    void nestedQuoting() throws IOException
    {
        assertStatements( "go `tick;'single;\"double;\nend`;\n", new CypherStatement( "go `tick;'single;\"double;\nend`;" ) );
    }

    @Test
    void mixCommandAndCypherWithSpacingsAdded() throws IOException
    {
        assertStatements(
                " :help me \n cypher me up \n :scotty \n ; \n :do it now! \n",
                new CommandStatement( ":help", List.of( "me" ) ),
                new CypherStatement( "cypher me up \n :scotty \n ;" ),
                new CommandStatement( ":do", List.of( "it", "now!" ) )
        );
    }

    @Test
    void commentHandlingIfSemicolon() throws IOException
    {
        assertStatements(
            " first // ;\n// /* ;\n third ; // actually a semicolon here\n",
            new CypherStatement( "first // ;\n// /* ;\n third ;" )
        );
    }

    @Test
    void backslashDeadInBlockQuote() throws IOException
    {
        assertStatements(
                "/* block \\*/\nCREATE ();",
                new CypherStatement( "CREATE ();" )
        );
    }

    @Test
    void commentInQuote() throws IOException
    {
        assertStatements( "` here // comment `;", new CypherStatement( "` here // comment `;" ) );
    }

    @Test
    void blockCommentInQuote() throws IOException
    {
        assertStatements( "` here /* comment `;", new CypherStatement( "` here /* comment `;" ) );
    }

    @Test
    void quoteInComment() throws IOException
    {
        assertStatements( "// `;\n;", new CypherStatement( ";" ) );
    }

    @Test
    void quoteInBlockomment() throws IOException
    {
        assertStatements( "/* `;\n;*/\n;", new CypherStatement( ";" ) );
    }

    @Test
    void testReset() throws IOException
    {
        assertStatements( "bob", new IncompleteStatement( "bob" ) );
    }

    @Test
    void commentsBeforeBegin() throws IOException
    {
        assertStatements(
                "//comment \n:begin\nRETURN 42;\n:end\n",
                new CommandStatement( ":begin", List.of() ),
                new CypherStatement( "RETURN 42;" ),
                new CommandStatement( ":end", List.of() )
        );
    }

    @Test
    void trimWhiteSpace() throws IOException
    {
        assertStatements(
                "\t \r\n match (n) return n;\n\t    return 3;\t\r\n ",
                new CypherStatement( "match (n) return n;" ),
                new CypherStatement( "return 3;" )
        );
    }

    @Test
    void parseParamCommand() throws IOException
    {
        // We don't fully parse :param, but need to make sure we're not messing up the arguments that needs to be parsed later
        assertStatements(
                ":param key => 'value'",
                new CommandStatement( ":param", List.of( "key => 'value'" ) )
        );
        assertStatements(
                ":param `not valid but still` => `\"'strange thing ",
                new CommandStatement( ":param", List.of( "`not valid but still` => `\"'strange thing" ) )
        );
        assertStatements(
                ":param \t  whitespace all around        =>   and\t value  \t  ",
                new CommandStatement( ":param", List.of( "whitespace all around        =>   and\t value" ) )
        );
    }

    @Test
    void parseParamsCommand() throws IOException
    {
        assertStatements(
                ":params key  ",
                new CommandStatement( ":params", List.of( "key" ) )
        );
        assertStatements(
                ":params `key with whitespace`",
                new CommandStatement( ":params", List.of( "`key with whitespace`" ) )
        );
    }

    @Test
    void stripTrailingSemicolonOnCommands() throws IOException
    {
        assertStatements( ":command param1 param2;", new CommandStatement( ":command", List.of( "param1", "param2" ) ));
        assertStatements( ":command;", new CommandStatement( ":command", List.of() ));
        assertStatements( ":command;;", new CommandStatement( ":command", List.of() ) );
    }

    @Test
    void shouldParseCommandsAndArgs() throws IOException
    {
        assertStatements( ":help", new CommandStatement( ":help", List.of() ) );
        assertStatements( ":help :param", new CommandStatement( ":help", List.of( ":param" ) ) );
        assertStatements( "   :help    ", new CommandStatement( ":help", List.of() ) );
        assertStatements( "   :help    \n", new CommandStatement( ":help", List.of() ) );
        assertStatements( "   :help   arg1 arg2 ", new CommandStatement( ":help", List.of( "arg1", "arg2" ) ) );
    }

    private void assertStatements( String input, ParsedStatement... expected ) throws IOException
    {
        assertThat( parser.parse( input ).statements() , is( asList( expected ) ) );
    }

}
