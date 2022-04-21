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
package org.neo4j.shell.terminal;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matcher;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.Parser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
            ":log",
            ":param",
            ":params",
            ":rollback",
            ":source",
            ":use",
            ":impersonate");

    @BeforeEach
    void setup() {
        var transactionHandler = mock(TransactionHandler.class);
        parameters = ParameterService.create(transactionHandler);
        completer =
                new JlineCompleter(new CommandHelper.CommandFactoryHelper(), CypherLanguageService.get(), parameters);
        parser = new StatementJlineParser(new ShellStatementParser(), new JavaCcCypherLanguageService());
        parser.setEnableStatementParsing(true);
    }

    @Test
    void completeCommands() {
        assertThat(complete(":"), containsInAnyOrder(allCommands.toArray()));
    }

    @Test
    void completeBlankSanity() {
        assertThat(complete(""), emptyStatementMatcher());
    }

    @Test
    void completeCypherWhereSanity() {
        var query = "match (myFirstNode:SomeLabel)-[myRelationship]->(mySecondNode) where ";

        var cypher = hasItems("IN", "ENDS", "STARTS", "CONTAINS", "IS", "RETURN");
        var identifiers = hasItems("myFirstNode", "myRelationship", "mySecondNode");

        assertThat(complete(query), allOf(cypher, identifiers));
    }

    @Test
    void completeCypherWhSanity() {
        assertThat(complete("match (n) wh"), hasItems("WHERE", "n"));
    }

    @Test
    void completeCypherMaSanity() {
        assertThat(complete("ma"), hasItems("MATCH"));
    }

    @Test
    void completeCypherAlterSanity() {
        assertThat(complete("alter "), hasItems("USER", "DATABASE"));
    }

    @Test
    void completeCypherParametersSanity() {
        parameters.setParameter(new Parameter("myParam", "1", 1L));
        parameters.setParameter(new Parameter("myOtherParam", "2", 2L));

        assertThat(complete(""), hasItems("$myParam", "$myOtherParam"));
        assertThat(complete("match (n) where n.p = "), hasItems("$myParam", "$myOtherParam"));
        assertThat(complete("match (n) where n.p = $"), hasItems("$myParam", "$myOtherParam"));
        assertThat(
                complete("match (n) where n.p = $myParam && n.p2 = $myOtherParam "),
                allOf(not(hasItems("myParam", "myOtherParam")), hasItems("$myParam", "$myOtherParam")));
    }

    @Test
    void completeSecondCypherStatementSanity() {
        assertThat(complete("return 1;"), emptyStatementMatcher());
        assertThat(complete("return 1; "), emptyStatementMatcher());
        assertThat(complete("return 1;ret"), hasItems("RETURN"));
    }

    private List<String> complete(String line) {
        var parsed = parser.parse(line, line.length(), Parser.ParseContext.COMPLETE);
        var candidates = new ArrayList<Candidate>();
        completer.complete(lineReader, parsed, candidates);
        Matcher<Iterable<String>> bla = hasItems("HEj", "hj");
        return candidates.stream().map(Candidate::value).toList();
    }

    private Matcher<Iterable<String>> emptyStatementMatcher() {
        var firstKeywords = hasItems(
                "CREATE", "MATCH", "DROP", "UNWIND", "RETURN", "WITH", "LOAD", "ALTER", "RENAME", "SHOW", "START",
                "STOP");
        var commands = hasItems(allCommands.toArray(new String[0]));
        return allOf(firstKeywords, commands);
    }
}
