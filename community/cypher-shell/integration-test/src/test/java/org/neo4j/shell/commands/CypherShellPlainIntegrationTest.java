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
package org.neo4j.shell.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;

class CypherShellPlainIntegrationTest extends CypherShellIntegrationTest {
    private final StringLinePrinter linePrinter = new StringLinePrinter();

    @BeforeEach
    void setUp() throws Exception {
        linePrinter.clear();
        var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, true, 1000, false));
        var boltHandler = new BoltStateHandler(true);
        var parameters = ParameterService.create(boltHandler);
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);
        connect("neo");
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            shell.execute(CypherStatement.complete("MATCH (n) DETACH DELETE (n)"));
        } finally {
            shell.disconnect();
        }
    }

    @Test
    void cypherWithProfileStatements() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("CYPHER RUNTIME=INTERPRETED PROFILE RETURN null"));

        // then
        String actual = linePrinter.output();
        //      This assertion checks everything except for time and cypher
        assertThat(actual)
                .contains(
                        "Plan: \"PROFILE\"",
                        "Statement: \"READ_ONLY\"",
                        "Planner: \"COST\"",
                        runningAtLeast("5.0") ? "Runtime: \"SLOTTED\"" : "Runtime: \"INTERPRETED\"",
                        "DbHits: 0",
                        "Rows: 1",
                        "null",
                        "NULL");
    }

    @Test
    void cypherWithProfileWithMemory() throws CommandException {
        // given
        // Memory profile are only available from 4.1
        assumeTrue(runningAtLeast("4.1"));

        // when
        shell.execute(CypherStatement.complete("CYPHER RUNTIME=INTERPRETED PROFILE RETURN null"));

        // then
        String actual = linePrinter.output();
        System.out.println(actual);
        assertThat(actual).contains("Memory (Bytes): ");
    }

    @Test
    void cypherWithExplainStatements() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("CYPHER RUNTIME=SLOTTED EXPLAIN RETURN null"));

        // then
        String actual = linePrinter.output();
        //      This assertion checks everything except for time and cypher
        assertThat(actual).contains("Plan: \"EXPLAIN\"");
        assertThat(actual).contains("Statement: \"READ_ONLY\"");
        assertThat(actual).contains("Planner: \"COST\"");
        assertThat(actual).contains("Runtime: \"SLOTTED\"");
    }
}
