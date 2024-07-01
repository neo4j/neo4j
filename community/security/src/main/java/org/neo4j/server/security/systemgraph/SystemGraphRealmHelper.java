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
package org.neo4j.server.security.systemgraph;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.FormatException;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;

public class SystemGraphRealmHelper {
    public static final String NATIVE_AUTH = AuthToken.NATIVE_REALM;

    private final Suppliers.Lazy<GraphDatabaseService> systemSupplier;
    private final SecureHasher secureHasher;

    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    public static final String IS_SUSPENDED = "is_suspended";

    public static final String DEFAULT_DATABASE = "default_database";

    public SystemGraphRealmHelper(Suppliers.Lazy<GraphDatabaseService> systemSupplier, SecureHasher secureHasher) {
        this.systemSupplier = systemSupplier;
        this.secureHasher = secureHasher;
    }

    public User getUser(String username) throws InvalidArgumentsException, FormatException {
        try (Transaction tx = getSystemDb().beginTx()) {
            Node userNode = tx.findNode(Label.label("User"), "name", username);

            if (userNode == null) {
                throw new InvalidArgumentsException("User '" + username + "' does not exist.");
            }

            Credential credential =
                    SystemGraphCredential.deserialize((String) userNode.getProperty("credentials"), secureHasher);

            String id = userNode.hasProperty("id") ? (String) userNode.getProperty("id") : null;

            boolean requirePasswordChange = (boolean) userNode.getProperty("passwordChangeRequired");
            boolean suspended = (boolean) userNode.getProperty("suspended");
            tx.commit();
            return new User(username, id, credential, requirePasswordChange, suspended);
        } catch (NotFoundException n) {
            // Can occur if the user was dropped by another thread after the null check.
            throw new InvalidArgumentsException("User '" + username + "' does not exist.");
        }
    }

    public GraphDatabaseService getSystemDb() {
        return systemSupplier.get();
    }

    public static Suppliers.Lazy<GraphDatabaseService> makeSystemSupplier(
            DatabaseContextProvider<?> databaseContextProvider) {
        return Suppliers.lazySingleton(() -> databaseContextProvider
                .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(() ->
                        new AuthProviderFailedException("No database called `" + SYSTEM_DATABASE_NAME + "` was found."))
                .databaseFacade());
    }
}
