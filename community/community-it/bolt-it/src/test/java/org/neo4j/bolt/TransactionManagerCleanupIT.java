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
package org.neo4j.bolt;

import static org.neo4j.bolt.test.util.ServerUtil.resolveDependency;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class TransactionManagerCleanupIT {

    @Inject
    private Neo4jWithSocket server;

    @ProtocolTest
    void shouldIncreaseAndDecreaseTxCount(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        var txManager = resolveDependency(server, TransactionManager.class);

        Assertions.assertThat(txManager.getTransactionCount()).isEqualTo(0);

        connection.send(wire.begin());
        assertThat(connection).receivesSuccess();

        Assertions.assertThat(txManager.getTransactionCount()).isEqualTo(1);

        connection.send(wire.rollback());
        assertThat(connection).receivesSuccess();

        Assertions.assertThat(txManager.getTransactionCount()).isEqualTo(0);
    }
}
