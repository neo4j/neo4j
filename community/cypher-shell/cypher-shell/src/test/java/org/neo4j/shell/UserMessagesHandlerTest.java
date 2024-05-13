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
package org.neo4j.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserMessagesHandlerTest {
    private Connector connector;

    @BeforeEach
    void setup() {
        connector = mock(Connector.class);
        when(connector.username()).thenReturn("bob");
        when(connector.getProtocolVersion()).thenReturn("3.0");
        final var connectionConfig = testConnectionConfig("bolt://some.place.com:99");
        when(connector.connectionConfig()).thenReturn(connectionConfig);
    }

    @Test
    void welcomeMessageTest() {
        UserMessagesHandler userMessagesHandler = new UserMessagesHandler(connector);
        assertEquals(
                """
                        Connected to Neo4j using Bolt protocol version 3.0 at [1mbolt://some.place.com:99[22m as user [1mbob[22m.
                        Type [1m:help[22m for a list of available commands or [1m:exit[22m to exit the shell.
                        Note that Cypher queries must end with a [1msemicolon.[22;0m""",
                userMessagesHandler.getWelcomeMessage());
    }

    @Test
    void welcomeWithImpersonation() {
        when(connector.impersonatedUser()).thenReturn(Optional.of("impersonated_user"));
        UserMessagesHandler userMessagesHandler = new UserMessagesHandler(connector);
        assertEquals(
                """
                        Connected to Neo4j using Bolt protocol version 3.0 at [1mbolt://some.place.com:99[22m as user [1mbob[22;33m impersonating [39;1mimpersonated_user[22m.
                        Type [1m:help[22m for a list of available commands or [1m:exit[22m to exit the shell.
                        Note that Cypher queries must end with a [1msemicolon.[22;0m""",
                userMessagesHandler.getWelcomeMessage());
    }

    @Test
    void exitMessageTest() {
        assertEquals("\nBye!", UserMessagesHandler.getExitMessage());
    }
}
