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
package org.neo4j.bolt.protocol.common.connector.connection;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.internal.kernel.api.security.LoginContext;

/**
 * Encapsulates owner-specific connection functionality.
 * <p />
 * Note: The methods within this interface are generally not thread-safe and should only ever be
 * accessed from within the context of a Bolt worker.
 */
public interface ConnectionHandle extends Connection {

    /**
     * Enables a designated feature for use with this connection.
     *
     * @param features list of features.
     * @param userAgent the user agent string
     * @param routingContext a routing context providing information about routing support and
     *                       selected routing policies.
     * @return a list of enabled features
     */
    List<Feature> negotiate(
            List<Feature> features,
            String userAgent,
            RoutingContext routingContext,
            NotificationsConfig notificationsConfig,
            Map<String, String> boltAgent);

    /**
     * Retrieves the login context which is currently used by this connection to authenticate operations.
     *
     * @return a login context or null if no authentication has been performed on this connection.
     */
    @Override
    LoginContext loginContext();

    /**
     * Retrieves the routing context which shall apply to transactions within this connection.
     *
     * @return a routing context.
     */
    @Override
    RoutingContext routingContext();

    /**
     * Authenticates this connection using a given authentication token.
     *
     * @param token     an authentication token.
     * @return null or an authentication flag which notifies the client about additional requirements or limitations if
     * necessary.
     * @throws AuthenticationException when the given token is invalid or authentication fails.
     * @see AuthenticationFlag for detailed information on the available authentication flags.
     */
    @Override
    AuthenticationFlag logon(Map<String, Object> token) throws AuthenticationException;

    @Override
    void impersonate(String userToImpersonate) throws AuthenticationException;

    /**
     * Logs off this connection, so it is ready to accept new authentication.
     */
    @Override
    void logoff();

    Transaction beginTransaction(
            TransactionType type,
            String databaseName,
            AccessMode mode,
            List<String> bookmarks,
            Duration timeout,
            Map<String, Object> metadata,
            NotificationsConfig transactionNotificationsConfig)
            throws TransactionException;

    Optional<Transaction> transaction();

    void closeTransaction() throws TransactionException;
}
