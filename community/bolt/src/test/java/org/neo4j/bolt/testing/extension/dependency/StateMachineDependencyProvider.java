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
package org.neo4j.bolt.testing.extension.dependency;

import java.util.Optional;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.time.SystemNanoClock;

public interface StateMachineDependencyProvider {

    /**
     * Retrieves the service provider which facilitates the communication with the database
     * instance.
     * <p />
     * This function is expected to return the same object instance throughout the lifetime of this
     * provider instance.
     *
     * @param ctx a test extension context.
     * @return a database management service provider.
     */
    BoltGraphDatabaseManagementServiceSPI spi(ExtensionContext ctx);

    /**
     * Retrieves the clock which provides time-keeping capabilities for the executing test(s).
     * <p />
     * This function is expected to return the same object instance throughout the lifetime of
     * this provider instance.
     *
     * @param ctx a test extension context.
     * @return a time source.
     */
    SystemNanoClock clock(ExtensionContext ctx);

    /**
     * Creates a new connection for use with a newly constructed finite state machine instance.
     * <p />
     * This function <em>may</em> to return a <em>new instance</em> for each respective call thus
     * providing separation for each respective state machine instance in partially mocked
     * environments.
     *
     * @param ctx a test extension context.
     * @return a dummy connection.
     */
    ConnectionHandle connection(ExtensionContext ctx);

    /**
     * Retrieves the latest transaction to be committed within the database instance.
     * <p />
     * When transaction management is unavailable within the provided context (e.g. transaction
     * management has been stubbed), an empty optional may be returned instead.
     *
     * @param ctx a test extension context.
     * @return a transaction identifier or an empty optional.
     */
    default Optional<Long> lastTransactionId(ExtensionContext ctx) {
        return Optional.empty();
    }

    default Optional<TransactionManager> transactionManager() {
        return Optional.empty();
    }

    /**
     * Initializes the database context for testing.
     * <p />
     * This function is guaranteed to be invoked prior to other interactions with this interface.
     *
     * @param context a test extension context.
     * @param testInfo additional information on the executing test.
     */
    default void init(ExtensionContext context, TestInfo testInfo) {}

    /**
     * Releases database resources.
     * <p />
     * This function is guaranteed to be the last interaction with this interface.
     *
     * @param context a test extension context.
     */
    default void close(ExtensionContext context) {}
}
