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
package org.neo4j.shell.terminal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jline.reader.Parser.ParseContext.COMPLETE;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser.ParseContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.parser.CypherLanguageService;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.BlankCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CommandCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CypherCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.SimpleParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.UnparsedLine;

// More test coverage of StatementJlineParser in JlineCompleterTest
class StatementJlineParserTest {
    private final StatementJlineParser parser =
            new StatementJlineParser(new ShellStatementParser(), CypherLanguageService.get());

    @BeforeEach
    void setup() {
        parser.setEnableStatementParsing(true);
    }

    @Test
    void disableParsing() {
        parser.setEnableStatementParsing(false);
        assertThat(parse("bla; bla; bla;")).isEqualTo(new UnparsedLine("bla; bla; bla;", 14));
        assertThat(parse(":bla")).isEqualTo(new UnparsedLine(":bla", 4));
        assertThat(parse(":bla", COMPLETE)).isEqualTo(new UnparsedLine(":bla", 4));
        assertThat(parse("return 1; return 2")).isEqualTo(new UnparsedLine("return 1; return 2", 18));
    }

    @Test
    void parseCommandsForExecution() {
        assertSimpleParse(":bla", command(":bla", List.of(), true, 0, 3));
        assertSimpleParse(":param key => 'value' ", command(":param", List.of("key => 'value'"), true, 0, 21));
        assertSimpleParse(":param \t   \n", command(":param", List.of(), true, 0, 10));
        assertSimpleParse(":param \t clear  \n", command(":param", List.of("clear"), true, 0, 15));
        assertSimpleParse(":param \t list  \n", command(":param", List.of("list"), true, 0, 14));
        assertSimpleParse(":param {\n  a:1\n}\n", command(":param", List.of("{\n  a:1\n}"), true, 0, 15));
        assertSimpleParse(":param {\n  a:2\n}}\n", command(":param", List.of("{\n  a:2\n}}"), true, 0, 16));
        assertSimpleParse(
                ":param {\n  a:'}'\n,\nb:'}'}", command(":param", List.of("{\n  a:'}'\n,\nb:'}'}"), true, 0, 24));
        assertSimpleParse(
                ":param {\n\n/* } */\n,\nb:'}'\n}",
                command(":param", List.of("{\n\n/* } */\n,\nb:'}'\n}"), true, 0, 26));
    }

    @Test
    void incompleteParams() {
        assertIncomplete(":param {");
        assertIncomplete(":param\t {");
        assertIncomplete(":param {a:3,b:{}");
        assertIncomplete(":param {\n//}\n");
        assertIncomplete(":param {\na:a\n/*}/*\n");
        assertIncomplete(":param {a:'}'");
        assertIncomplete(":param {a:'}'//}");
    }

    @Test
    void parseCypherForExecution() {
        assertSimpleParse("return 1;", cypher("return 1"));
        assertSimpleParse(
                "/* comment; */ match (a) return a; // Comment;",
                new CypherStatement("match (a) return a", true, 15, 32));
        assertSimpleParse("match (a) /* hi */ return a;", cypher("match (a) /* hi */ return a"));
        assertSimpleParse(
                "return 1; return 2;",
                new CypherStatement("return 1", true, 0, 7),
                new CypherStatement("return 2", true, 10, 17));
    }

    @Test
    void parseWithLineContinuation() {
        assertThrows(EOFError.class, () -> parse("return 1; return 2"));
    }

    @Test
    void parseCommandForCompletion() {
        assertThat(parse(":", COMPLETE)).isEqualTo(new CommandCompletion(command(":", List.of(), true, 0, 0), ":", 1));
        assertThat(parse(":hel", COMPLETE))
                .isEqualTo(new CommandCompletion(command(":hel", List.of(), true, 0, 3), ":hel", 4));
    }

    @Test
    void dontCompleteCommandWithParam() {
        assertThat(parse(":param myParam", COMPLETE)).isEqualTo(new BlankCompletion(":param myParam", 14));
        assertThat(parse(":help :pa", COMPLETE)).isEqualTo(new BlankCompletion(":help :pa", 9));
        assertThat(parse(":help :param\n", COMPLETE)).isEqualTo(new BlankCompletion(":help :param\n", 13));
    }

    @Test
    void parseCypherForCompletion() {
        var query = "match (a)-[r]->(b) ";
        var parsed = (CypherCompletion) parse(query, COMPLETE);

        var expectedStatement = new CypherStatement(query, false, 0, 18);
        assertThat(parsed.statement()).isEqualTo(expectedStatement);
        assertThat(parsed.word()).isEqualTo("");
        assertThat(parsed.line()).isEqualTo(query);
        assertThat(parsed.cursor()).isEqualTo(query.length());
    }

    @Test
    void completeEmptyStatement() {
        assertThat(parse("", COMPLETE)).isEqualTo(new BlankCompletion("", 0));
        assertThat(parse("return 1;", COMPLETE)).isEqualTo(new BlankCompletion("return 1;", 9));
    }

    private CypherStatement cypher(String cypher) {
        return new CypherStatement(cypher, true, 0, cypher.length() - 1);
    }

    private CommandStatement command(String command, List<String> args, boolean complete, int begin, int end) {
        return new CommandStatement(command, args, complete, begin, end);
    }

    private ParsedLine parse(String line) {
        return parser.parse(line, line.length());
    }

    private ParsedLine parse(String line, ParseContext context) {
        return parser.parse(line, line.length(), context);
    }

    private void assertIncomplete(String line) {
        assertThrows(org.jline.reader.EOFError.class, () -> parse(line));
    }

    private void assertSimpleParse(String line, StatementParser.ParsedStatement... statements) {
        assertThat(parse(line))
                .isEqualTo(new SimpleParsedStatements(new ParsedStatements(List.of(statements)), line, line.length()));
    }
}
