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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;

class CypherShellFailureIntegrationTest extends CypherShellIntegrationTest {
    private final StringLinePrinter linePrinter = new StringLinePrinter();

    @BeforeEach
    void setUp() {
        linePrinter.clear();
        var printer = new PrettyPrinter(new PrettyConfig(Format.VERBOSE, true, 1000, false));
        var boltHandler = new BoltStateHandler(true, AccessMode.WRITE);
        var parameters = mock(ParameterService.class);
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);
    }

    @AfterEach
    void cleanUp() {
        shell.disconnect();
    }

    @Test
    void cypherWithNoPasswordShouldReturnValidError() {
        AuthenticationException exception = assertThrows(AuthenticationException.class, () -> connect(""));
        assertThat(exception).hasMessageContaining("The client is unauthorized due to authentication failure.");
    }
}
