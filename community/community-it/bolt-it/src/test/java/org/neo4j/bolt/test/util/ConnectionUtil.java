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
package org.neo4j.bolt.test.util;

import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.test.assertion.Assert;

public class ConnectionUtil {

    /**
     * Assert that eventually a new connection is created and the work is executed.
     * <p/>
     * The connection provider is used to create a new connection.
     * The work will be executed on the new connection.
     * The connection will be closed after the work is executed.
     * This process is executed until successful.
     *
     * @param connectionProvider The connection provider used for creating connections
     * @param work The work to execute
     */
    public static void assertEventuallyNewConnectionProcessWork(
            ConnectionProvider connectionProvider, ThrowableConsumer<BoltTestConnection> work) {
        Assert.awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                work.accept(connection);
            }
        });
    }

    /**
     * Assert that eventually a new connection is created, the work is executed, and the connection is returned.
     * <p/>
     * The connection provider is used to create a new connection.
     * The work will be executed on the new connection.
     * This process is executed until successful.
     * Then the connection is returned.
     *
     * @param connectionProvider The connection provider used for creating connections
     * @param work The work to execute
     * @return The connection that was created and used for the work
     */
    public static BoltTestConnection assertEventuallyNewConnectionProcessWorkAndReturned(
            ConnectionProvider connectionProvider, ThrowableConsumer<BoltTestConnection> work) {
        AtomicReference<BoltTestConnection> connectionRef = new AtomicReference<>();
        Assert.awaitUntilAsserted(() -> {
            var connection = connectionProvider.create();
            try {
                work.accept(connection);
            } catch (Exception e) {
                connection.close();
                throw e;
            }
            connectionRef.set(connection);
        });

        return connectionRef.get();
    }

    public interface ThrowableConsumer<T> {
        void accept(T t) throws Exception;
    }
}
