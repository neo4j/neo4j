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
package org.neo4j.bolt.protocol.common.connector.accounting.error;

import java.io.IOException;
import java.time.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.SimpleLogService;

class CircuitBreakerErrorAccountantTest {

    private AssertableLogProvider logProvider;

    private CircuitBreakerErrorAccountant accountant;

    @BeforeEach
    void prepare() {
        this.logProvider = new AssertableLogProvider();

        this.accountant = new CircuitBreakerErrorAccountant(
                8, 100, 200, 2, 100, 200, Clock.systemUTC(), new SimpleLogService(this.logProvider));
    }

    @Test
    void shouldLogNetworkAborts() {
        var connection = ConnectionMockFactory.newInstance("bolt-123");
        var ex = new IOException("Connection reset by test");

        this.accountant.notifyNetworkAbort(connection, ex);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.DEBUG)
                .containsMessageWithException("[bolt-123] Terminating connection due to network error", ex);
    }

    @Test
    void shouldLogThreadStarvation() {
        var connection = ConnectionMockFactory.newInstance("bolt-123");
        var ex = new IOException("Connection reset by test");

        this.accountant.notifyThreadStarvation(connection, ex);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.DEBUG)
                .containsMessageWithArguments(
                        "Unable to schedule for execution since there are no available threads to serve it at the moment.",
                        "bolt-123");
    }
}
