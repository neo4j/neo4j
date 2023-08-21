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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.values.storable.Values.intValue;

import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.values.virtual.MapValueBuilder;

@TestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class ResetLockedNodeIT {

    @Timeout(30)
    @TransportTest
    void shouldErrorWhenResettingAConnectionWaitingOnALock(
            BoltWire wire,
            @Authenticated TransportConnection setupConnection,
            @Authenticated TransportConnection connectionA,
            @Authenticated TransportConnection connectionB)
            throws Exception {
        setupConnection.send(wire.run("CREATE (n {id: 123})")).send(wire.pull());
        BoltConnectionAssertions.assertThat(setupConnection).receivesSuccess(2);
        setupConnection.send(wire.goodbye()).close();

        var paramsA = new MapValueBuilder();
        paramsA.add("currentId", intValue(123));
        paramsA.add("newId", intValue(456));

        var paramsB = new MapValueBuilder();
        paramsB.add("currentId", intValue(123));
        paramsB.add("newId", intValue(789));

        // Given a connectionA has acquired a lock whilst updating a node.
        connectionA
                .send(wire.begin())
                .send(wire.run("MATCH (n {id: $currentId}) SET n.id = $newId", paramsA.build()))
                .send(wire.pull(100));

        assertThat(connectionA).receivesSuccess(3);

        // And when connectionB is blocked waiting to acquire the same lock.
        connectionB
                .send(wire.run("MATCH (n {id: $currentId}) SET n.id = $newId", paramsB.build()))
                .send(wire.pull(100));

        Thread.sleep(300); // Allow time for connection B to become blocked on a lock.

        // When the connectionB is RESET
        connectionB.send(wire.reset());

        // Then that connection receives an error
        assertThat(connectionB)
                .receivesFailure(Status.Transaction.LockClientStopped)
                .receivesIgnored()
                .receivesSuccess();

        // And the other connection can commit successfully.
        connectionA.send(wire.commit());

        assertThat(connectionA).receivesSuccess();
    }
}
