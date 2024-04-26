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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;
import static org.neo4j.shell.test.Util.testConnectionConfig;
import static org.neo4j.shell.util.Versions.majorVersion;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;

class CypherShellMultiDatabaseIntegrationTest {
    private final StringLinePrinter linePrinter = new StringLinePrinter();
    private Command useCommand;
    private Command beginCommand;
    private Command rollbackCommand;
    private CypherShell shell;

    @BeforeEach
    void setUp() throws Exception {
        linePrinter.clear();
        var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, true, 1000, false));
        var boltHandler = new BoltStateHandler(false, AccessMode.WRITE);
        var parameters = ParameterService.create(boltHandler);
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);
        useCommand = new Use(shell);
        beginCommand = new Begin(shell);
        rollbackCommand = new Rollback(shell);

        shell.connect(testConnectionConfig("bolt://localhost:7687").withUsernameAndPassword("neo4j", "neo"));

        // Multiple databases are only available from 4.0
        assumeTrue(majorVersion(shell.getServerVersion()) >= 4);
    }

    @AfterEach
    void cleanUp() {
        shell.disconnect();
    }

    @Test
    void switchingToSystemDatabaseWorks() throws CommandException {
        useCommand.execute(List.of(SYSTEM_DB_NAME));

        assertThat(linePrinter.output()).isEmpty();
        assertOnSystemDB();
    }

    @Test
    void switchingToSystemDatabaseIsNotCaseSensitive() throws CommandException {
        useCommand.execute(List.of("SyStEm"));

        assertThat(linePrinter.output()).isEmpty();
        assertOnSystemDB();
    }

    @Test
    void switchingToSystemDatabaseAndBackToNeo4jWorks() throws CommandException {
        useCommand.execute(List.of(SYSTEM_DB_NAME));
        useCommand.execute(List.of(DEFAULT_DEFAULT_DB_NAME));

        assertThat(linePrinter.output()).isEmpty();
        assertOnRegularDB();
    }

    @Test
    void switchingToSystemDatabaseAndBackToDefaultWorks() throws CommandException {
        useCommand.execute(List.of(SYSTEM_DB_NAME));
        useCommand.execute(List.of(ABSENT_DB_NAME));

        assertThat(linePrinter.output()).isEmpty();
        assertOnRegularDB();
    }

    @Test
    void switchingDatabaseInOpenTransactionShouldFail() throws CommandException {
        beginCommand.execute(List.of());
        assertThatThrownBy(() -> useCommand.execute(List.of("another_database")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("There is an open transaction.");
    }

    @Test
    void switchingDatabaseAfterRollbackTransactionWorks() throws CommandException {
        beginCommand.execute(List.of());
        rollbackCommand.execute(List.of());
        useCommand.execute(List.of(SYSTEM_DB_NAME));

        assertThat(linePrinter.output()).isEmpty();
        assertOnSystemDB();
    }

    @Test
    void switchingToNonExistingDatabaseShouldGiveErrorResponseFromServer() throws CommandException {
        useCommand.execute(List.of(SYSTEM_DB_NAME));

        try {
            useCommand.execute(List.of("this_database_name_does_not_exist_in_test_container"));
            fail("No ClientException thrown");
        } catch (ClientException e) {
            // In non-interactive we want to switch even if the database does not exist (in case we don't have
            // fail-fast)
            assertOnNoValidDB();
        }
    }

    @Test
    void switchingToNonExistingDatabaseShouldGiveErrorResponseFromServerInteractive() throws CommandException {
        var boltHandler = new BoltStateHandler(true, AccessMode.WRITE);
        var parameters = ParameterService.create(boltHandler);
        var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, true, 1000, false));
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);
        useCommand = new Use(shell);
        shell.connect(testConnectionConfig("bolt://localhost:7687").withUsernameAndPassword("neo4j", "neo"));

        useCommand.execute(List.of(SYSTEM_DB_NAME));

        try {
            useCommand.execute(List.of("this_database_name_does_not_exist_in_test_container"));
            fail("No ClientException thrown");
        } catch (ClientException e) {
            // In interactive we do not want to switch if the database does not exist
            assertOnSystemDB();
        }
    }

    // HELPERS

    private void assertOnRegularDB() throws CommandException {
        shell.execute(CypherStatement.complete("RETURN 'toadstool'"));
        assertThat(linePrinter.output()).contains("toadstool");
    }

    private void assertOnSystemDB() throws CommandException {
        shell.execute(CypherStatement.complete("SHOW DATABASES"));
        assertThat(linePrinter.output()).contains("neo4j", "system");
    }

    private void assertOnNoValidDB() {
        assertThrows(ClientException.class, () -> shell.execute(CypherStatement.complete("RETURN 1")));
    }
}
