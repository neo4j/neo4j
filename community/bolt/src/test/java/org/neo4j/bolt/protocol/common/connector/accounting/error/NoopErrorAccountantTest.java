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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.SimpleLogService;

class NoopErrorAccountantTest {

    private AssertableLogProvider logProvider;

    private NoopErrorAccountant errorAccountant;

    @BeforeEach
    void prepare() {
        this.logProvider = new AssertableLogProvider();

        this.errorAccountant = new NoopErrorAccountant(new SimpleLogService(this.logProvider));
    }

    @Test
    void shouldLogNetworkAborts() {
        var connection = ConnectionMockFactory.newInstance("bolt-234");
        var ex = new IOException("Test caused connection abort");

        this.errorAccountant.notifyNetworkAbort(connection, ex);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.WARN)
                .containsMessageWithException("[bolt-234] Terminating connection due to network error", ex);
    }

    @Test
    void shouldLogThreatStarvation() {
        var connection = ConnectionMockFactory.newInstance("bolt-234");
        var ex = new IOException("Test caused connection abort");

        this.errorAccountant.notifyThreadStarvation(connection, ex);

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.ERROR)
                .containsMessageWithArguments(
                        "[%s] Unable to schedule for execution since there are no available threads to serve it at the moment",
                        "bolt-234");
    }
}
