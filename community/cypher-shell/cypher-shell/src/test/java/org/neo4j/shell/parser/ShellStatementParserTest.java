/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;

class ShellStatementParserTest {
    private final ShellStatementParser parser = new ShellStatementParser();

    @Test
    void parseEmptyLineDoesNothing() throws IOException {
        assertStatements("");
    }

    @Test
    void parseNewLineDoesNothing() throws IOException {
        assertStatements("\n");
    }

    @Test
    void parseAShellCommand() throws IOException {
        var expected = new CommandStatement(":help", List.of("exit", "bob", "snob"), true, 2, 22);
        assertStatements("  :help exit bob snob  ", expected);
    }

    @Test
    void parseAShellCommandWithNewLine() throws IOException {
        var expected = command(":help", List.of("exit", "bob", "snob"), 2, 22);
        assertStatements("  :help exit bob snob  \n  ", expected);
    }

    @Test
    void parseIncompleteCypher() throws IOException {
        assertStatements("CREATE ()\n", new CypherStatement("CREATE ()\n", false, 0, 9));
    }

    @Test
    void parseCompleteCypher() throws IOException {
        assertStatements("CREATE (n)\nCREATE ();", cypher("CREATE (n)\nCREATE ()"));
    }

    @Test
    void parseMultipleCypherSingleLine() throws IOException {
        assertStatements("RETURN 1;RETURN 2;", cypher("RETURN 1"), cypher("RETURN 2", 9));
    }

    @Test
    void parseMultipleCypherMultipleLine() throws IOException {
        assertStatements("RETURN 1;\n RETURN 2;", cypher("RETURN 1"), cypher("RETURN 2", 11));
    }

    @Test
    void singleQuotedSemicolon() throws IOException {
        assertStatements("hello '\n;\n'\n;\n", cypher("hello '\n;\n'\n"));
    }

    @Test
    void backtickQuotedSemicolon() throws IOException {
        assertStatements("hello `\n;\n`\n;  \n", cypher("hello `\n;\n`\n"));
    }

    @Test
    void doubleQuotedSemicolon() throws IOException {
        assertStatements("hello \"\n;\n\"\n;   \n", cypher("hello \"\n;\n\"\n"));
    }

    @Test
    void escapedChars() throws IOException {
        assertStatements("one \\;\n\"two \\\"\n;\n\";\n", cypher("one \\;\n\"two \\\"\n;\n\""));
    }

    @Test
    void nestedQuoting() throws IOException {
        assertStatements("go `tick;'single\"double;\nend`;\n", cypher("go `tick;'single\"double;\nend`"));
    }

    @Test
    void mixCommandAndCypherWithSpacingsAdded() throws IOException {
        assertStatements(
                " :help me \n cypher me up \n :scotty \n ; \n :do it now! \n",
                command(":help", List.of("me"), 1, 9),
                cypher("cypher me up \n :scotty \n ", 12),
                command(":do", List.of("it", "now!"), 41, 52));
    }

    @Test
    void commentHandlingIfSemicolon() throws IOException {
        assertStatements(
                " first // ;\n// /* ;\n third ; // actually a semicolon here\n",
                cypher("first // ;\n// /* ;\n third ", 1));
    }

    @Test
    void backslashDeadInBlockQuote() throws IOException {
        assertStatements("/* block \\*/\nCREATE ();", cypher("CREATE ()", 13));
    }

    @Test
    void commentInQuote() throws IOException {
        assertStatements("` here // comment `;", cypher("` here // comment `"));
    }

    @Test
    void blockCommentInQuote() throws IOException {
        assertStatements("` here /* comment `;", cypher("` here /* comment `"));
    }

    @Test
    void quoteInComment() throws IOException {
        assertStatements("// `;\n;", cypher("", 6));
    }

    @Test
    void quoteInBlockomment() throws IOException {
        assertStatements("/* `;\n;*/\n;", cypher("", 10));
    }

    @Test
    void testReset() throws IOException {
        assertStatements("bob", incomplete("bob"));
    }

    @Test
    void commentsBeforeBegin() throws IOException {
        assertStatements(
                "//comment \n:begin\nRETURN 42;\n:end\n",
                command(":begin", List.of(), 11, 16),
                cypher("RETURN 42", 18),
                command(":end", List.of(), 29, 32));
    }

    @Test
    void trimWhiteSpace() throws IOException {
        assertStatements(
                "\t \r\n match (n) return n;\n\t    return 3;\t\r\n ",
                cypher("match (n) return n", 5),
                cypher("return 3", 30));
    }

    @Test
    void parseParamCommand() throws IOException {
        // We don't fully parse :param, but need to make sure we're not messing up the arguments that needs to be parsed
        // later
        assertStatements(":param key => 'value'", command(":param", List.of("key => 'value'"), true, 0, 20));
        assertStatements(
                ":param `not valid but still` => `\"'strange thing ",
                command(":param", List.of("`not valid but still` => `\"'strange thing"), true, 0, 48));
        assertStatements(
                ":param \t  whitespace all around        =>   and\t value  \t  ",
                command(":param", List.of("whitespace all around        =>   and\t value"), true, 0, 58));
    }

    @Test
    void parseMultiLineArrow() throws IOException {
        // Arrow syntax :param is not allowed to be muliline
        assertStatements(
                ":param key => \n'value'",
                command(":param", List.of("key =>"), true, 0, 13),
                new CypherStatement("'value'", false, 15, 21));
    }

    @Test
    void parseMultiLineParamCommand() throws IOException {
        assertStatements(":param {\na:1,\nb:2\n}", command(":param", List.of("{\na:1,\nb:2\n}"), true, 0, 18));
    }

    @Test
    void parseParamsCommand() throws IOException {
        assertStatements(":params key  ", command(":params", List.of("key"), true, 0, 12));
        assertStatements(
                ":params `key with whitespace`", command(":params", List.of("`key with whitespace`"), true, 0, 28));
    }

    @Test
    void stripTrailingSemicolonOnCommands() throws IOException {
        assertStatements(":command param1 param2;", command(":command", List.of("param1", "param2"), true, 0, 22));
        assertStatements(":command;", command(":command", List.of(), true, 0, 8));
        assertStatements(":command;;", command(":command", List.of(), true, 0, 9));
    }

    @Test
    void shouldParseCommandsAndArgs() throws IOException {
        assertStatements(":help", command(":help", List.of(), true, 0, 4));
        assertStatements(":help :param", command(":help", List.of(":param"), true, 0, 11));
        assertStatements("   :help    ", command(":help", List.of(), true, 3, 11));
        assertStatements("   :help    \n", command(":help", List.of(), 3, 11));
        assertStatements("   :help   arg1 arg2 ", command(":help", List.of("arg1", "arg2"), true, 3, 20));
    }

    @Test
    void shouldKeepTrackOfOffsets() throws IOException {
        assertStatements(
                "return 1;\nreturn 2;\n",
                new CypherStatement("return 1", true, 0, 7),
                new CypherStatement("return 2", true, 10, 17));
        assertStatements(
                "return\n1\n;return\n2;\n",
                new CypherStatement("return\n1\n", true, 0, 8),
                new CypherStatement("return\n2", true, 10, 17));
        assertStatements("return 1", new CypherStatement("return 1", false, 0, 7));
        assertStatements(
                "// Hi\n/* Hello */ return 1;\n// Hi\n/* Hello */ return 2;",
                new CypherStatement("return 1", true, 18, 25),
                new CypherStatement("return 2", true, 46, 53));
        assertStatements(
                ":help\n:help\r\n:help\r:help\n",
                command(":help", List.of(), 0, 4),
                command(":help", List.of(), 6, 10),
                command(":help", List.of(), 13, 17),
                command(":help", List.of(), 19, 23));
    }

    private ParsedStatement cypher(String cypher) {
        return new CypherStatement(cypher, true, 0, cypher.length() - 1);
    }

    private ParsedStatement cypher(String cypher, int begin) {
        return new CypherStatement(cypher, true, begin, begin + cypher.length() - 1);
    }

    private ParsedStatement incomplete(String cypher) {
        return new CypherStatement(cypher, false, 0, cypher.length() - 1);
    }

    private ParsedStatement command(String command, List<String> args, int begin, int end) {
        return new CommandStatement(command, args, true, begin, end);
    }

    private ParsedStatement command(String command, List<String> args, boolean complete, int begin, int end) {
        return new CommandStatement(command, args, complete, begin, end);
    }

    private void assertStatements(String input, ParsedStatement... expected) throws IOException {
        assertThat(parser.parse(input).statements()).containsExactly(expected);
    }
}
