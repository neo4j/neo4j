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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
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
import org.neo4j.shell.state.ErrorWhileInTransactionException;

class CypherShellTransactionIntegrationTest extends CypherShellIntegrationTest {
    private final StringLinePrinter linePrinter = new StringLinePrinter();
    private Command rollbackCommand;
    private Command commitCommand;
    private Command beginCommand;

    @BeforeEach
    void setUp() throws Exception {
        var printer = new PrettyPrinter(new PrettyConfig(Format.VERBOSE, true, 1000, false));
        var boltHandler = new BoltStateHandler(true);
        var parameters = mock(ParameterService.class);
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);
        rollbackCommand = new Rollback(shell);
        commitCommand = new Commit(shell);
        beginCommand = new Begin(shell);

        connect("neo");
        shell.execute(CypherStatement.complete("MATCH (n) DETACH DELETE (n)"));
    }

    @AfterEach
    void cleanUp() {
        shell.disconnect();
        linePrinter.clear();
    }

    @Test
    void rollbackScenario() throws CommandException {
        // given
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));

        // when
        beginCommand.execute(List.of());
        shell.execute(CypherStatement.complete("CREATE (:NotCreated)"));
        rollbackCommand.execute(List.of());

        // then
        shell.execute(CypherStatement.complete("MATCH (n) RETURN n"));

        String output = linePrinter.output();
        assertThat(output)
                .contains("| n ", "| (:TestPerson {name: \"Jane Smith\"}) |")
                .doesNotContain(":NotCreated");
    }

    @Test
    void failureInTxScenario() throws CommandException {
        // given
        beginCommand.execute(List.of());

        // then
        assertThatThrownBy(() -> shell.execute(CypherStatement.complete("RETURN 1/0")))
                .isInstanceOf(ErrorWhileInTransactionException.class)
                .hasMessageContainingAll(
                        "/ by zero",
                        "An error occurred while in an open transaction. The transaction will be rolled back and terminated.");
    }

    @Test
    void failureInTxScenarioWithCypherFollowing() throws CommandException {
        // given
        beginCommand.execute(List.of());
        try {
            shell.execute(CypherStatement.complete("RETURN 1/0"));
        } catch (ErrorWhileInTransactionException ignored) {
            // This is OK
        }

        // when
        shell.execute(CypherStatement.complete("RETURN 42"));

        // then
        assertThat(linePrinter.output()).contains("42");
    }

    @Test
    void failureInTxScenarioWithCommitFollowing() throws CommandException {
        // given
        beginCommand.execute(List.of());
        try {
            shell.execute(CypherStatement.complete("RETURN 1/0"));
        } catch (ErrorWhileInTransactionException ignored) {
            // This is OK
        }

        // then / when
        assertThatThrownBy(() -> commitCommand.execute(List.of()))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("There is no open transaction to commit");
    }

    @Test
    void failureInTxScenarioWithRollbackFollowing() throws CommandException {
        // given
        beginCommand.execute(List.of());
        try {
            shell.execute(CypherStatement.complete("RETURN 1/0"));
        } catch (ErrorWhileInTransactionException ignored) {
            // This is OK
        }

        //  then / when
        assertThatThrownBy(() -> rollbackCommand.execute(List.of()))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("There is no open transaction to rollback");
    }

    @Test
    void resetInFailedTxScenario() throws CommandException {
        // when
        beginCommand.execute(List.of());
        try {
            shell.execute(CypherStatement.complete("RETURN 1/0"));
        } catch (ErrorWhileInTransactionException ignored) {
            // This is OK
        }
        shell.reset();

        // then
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));
        shell.execute(CypherStatement.complete("MATCH (n) RETURN n"));

        String result = linePrinter.output();
        assertThat(result).contains("| (:TestPerson {name: \"Jane Smith\"}) |").doesNotContain(":NotCreated");
    }

    @Test
    void resetInTxScenario() throws CommandException {
        // when
        beginCommand.execute(List.of());
        shell.execute(CypherStatement.complete("CREATE (:NotCreated)"));
        shell.reset();

        // then
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));
        shell.execute(CypherStatement.complete("MATCH (n) RETURN n"));

        String result = linePrinter.output();
        assertThat(result).contains("| (:TestPerson {name: \"Jane Smith\"}) |").doesNotContain(":NotCreated");
    }

    @Test
    void commitScenario() throws CommandException {
        beginCommand.execute(List.of());
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Joe Smith\"})"));
        assertThat(linePrinter.output()).contains("0 rows\n");

        linePrinter.clear();
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));
        assertThat(linePrinter.output()).contains("0 rows\n");

        linePrinter.clear();
        shell.execute(CypherStatement.complete("MATCH (n:TestPerson) RETURN n ORDER BY n.name"));
        assertThat(linePrinter.output())
                .contains("\n| (:TestPerson {name: \"Jane Smith\"}) |\n| (:TestPerson {name: \"Joe Smith\"})  |\n");

        commitCommand.execute(List.of());
    }
}
