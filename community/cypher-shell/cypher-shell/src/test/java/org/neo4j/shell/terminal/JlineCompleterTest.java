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
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Condition;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.Parser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.parser.CypherLanguageService;
import org.neo4j.shell.parser.JavaCcCypherLanguageService;
import org.neo4j.shell.parser.ShellStatementParser;

class JlineCompleterTest {
    private ParameterService parameters;
    private JlineCompleter completer;
    private StatementJlineParser parser;
    private final LineReader lineReader = mock(LineReader.class);
    private final List<String> allCommands = List.of(
            ":begin",
            ":commit",
            ":connect",
            ":disconnect",
            ":exit",
            ":help",
            ":history",
            ":param",
            ":rollback",
            ":source",
            ":use",
            ":impersonate",
            ":sysinfo",
            ":access-mode");

    @BeforeEach
    void setup() {
        var transactionHandler = mock(TransactionHandler.class);
        parameters = ParameterService.create(transactionHandler);
        completer = new JlineCompleter(
                new CommandHelper.CommandFactoryHelper(), CypherLanguageService.get(), parameters, true);
        parser = new StatementJlineParser(new ShellStatementParser(), new JavaCcCypherLanguageService());
        parser.setEnableStatementParsing(true);
    }

    @Test
    void completeCommands() {
        assertThat(complete(":")).containsExactlyInAnyOrderElementsOf(allCommands);
    }

    @Test
    void completeBlankSanity() {
        assertThat(complete("")).is(emptyStatementMatcher());
    }

    @Test
    void completeCypherWhereSanity() {
        var query = "match (myFirstNode:SomeLabel)-[myRelationship]->(mySecondNode) where ";

        var cypher = List.of("IN", "ENDS", "STARTS", "CONTAINS", "IS", "RETURN");
        var identifiers = List.of("myFirstNode", "myRelationship", "mySecondNode");

        assertThat(complete(query)).containsAll(cypher).containsAll(identifiers);
    }

    @Test
    void completeCypherWhSanity() {
        assertThat(complete("match (n) wh")).contains("WHERE", "n");
    }

    @Test
    void completeCypherMaSanity() {
        assertThat(complete("ma")).contains("MATCH");
    }

    @Test
    void completeCypherAlterSanity() {
        assertThat(complete("alter ")).contains("USER", "DATABASE");
    }

    @Test
    void completeCypherParametersSanity() {
        parameters.setParameters(List.of(new Parameter("myParam", Values.value(1L))));
        parameters.setParameters(List.of(new Parameter("myOtherParam", Values.value(2L))));

        assertThat(complete("")).contains("$myParam", "$myOtherParam");
        assertThat(complete("match (n) where n.p = ")).contains("$myParam", "$myOtherParam");
        assertThat(complete("match (n) where n.p = $")).contains("$myParam", "$myOtherParam");
        assertThat(complete("match (n) where n.p = $myParam && n.p2 = $myOtherParam "))
                .doesNotContain("myParam", "myOtherParam")
                .contains("$myParam", "$myOtherParam");
    }

    @Test
    void completeSecondCypherStatementSanity() {
        assertThat(complete("return 1;")).is(emptyStatementMatcher());
        assertThat(complete("return 1; ")).is(emptyStatementMatcher());
        assertThat(complete("return 1;ret")).contains("RETURN");
    }

    private List<String> complete(String line) {
        var parsed = parser.parse(line, line.length(), Parser.ParseContext.COMPLETE);
        var candidates = new ArrayList<Candidate>();
        completer.complete(lineReader, parsed, candidates);
        return candidates.stream().map(Candidate::value).toList();
    }

    private Condition<List<? extends String>> emptyStatementMatcher() {
        var firstKeywords = List.of(
                "CREATE", "MATCH", "DROP", "UNWIND", "RETURN", "WITH", "LOAD", "ALTER", "RENAME", "SHOW", "START",
                "STOP");
        var commands = allCommands;
        return new Condition<>(
                items -> items.containsAll(firstKeywords) && items.containsAll(commands), "Empty statement matcher");
    }
}
