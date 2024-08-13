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
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_ID;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_LABEL;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_PROVIDER;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.HAS_AUTH;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_CREDENTIALS;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_EXPIRED;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_ID;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_NAME;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_SUSPENDED;

import java.util.HashSet;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.FormatException;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;

public class SecurityGraphHelper {
    public static final String NATIVE_AUTH = AuthToken.NATIVE_REALM;

    private final AbstractSecurityLog securityLog;
    private final Suppliers.Lazy<GraphDatabaseService> systemSupplier;
    private final SecureHasher secureHasher;

    public SecurityGraphHelper(
            Suppliers.Lazy<GraphDatabaseService> systemSupplier,
            SecureHasher secureHasher,
            AbstractSecurityLog securityLog) {
        this.systemSupplier = systemSupplier;
        this.secureHasher = secureHasher;
        this.securityLog = securityLog;
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

    /**
     * Lookup a user information from username, returns null if the user does not exist.
     *
     * @param username username
     * @return user record containing user and auth information
     */
    public User getUserByName(String username) {
        securityLog.debug(String.format("Looking up user '%s'", username));
        try (var tx = systemSupplier.get().beginTx()) {
            Node userNode = tx.findNode(USER_LABEL, USER_NAME, username);
            if (userNode == null) {
                securityLog.debug(String.format("User '%s' not found", username));
                return null;
            }
            return getUser(userNode);
        } catch (NotFoundException n) {
            // Can occur if the user was dropped by another thread after the null check.
            securityLog.debug(String.format("User '%s' not found", username));
            return null;
        }
    }

    /**
     * Lookup a user information from user id, returns null if the user does not exist.
     *
     * @param uuid user id
     * @return user record containing user and auth information
     */
    public User getUserById(String uuid) {
        securityLog.debug(String.format("Looking up user with id '%s'", uuid));
        if (uuid == null) {
            securityLog.debug("Cannot look up user with id = null");
            return null;
        }
        try (var tx = systemSupplier.get().beginTx()) {
            Node userNode = tx.findNode(USER_LABEL, USER_ID, uuid);
            if (userNode == null) {
                securityLog.debug(String.format("User with id '%s' not found", uuid));
                return null;
            }
            return getUser(userNode);
        } catch (NotFoundException n) {
            // Can occur if the user was dropped by another thread after the null check.
            securityLog.debug(String.format("User with id '%s' not found", uuid));
            return null;
        }
    }

    private User getUser(Node userNode) {
        var userId = (String) userNode.getProperty(USER_ID);
        boolean suspended = (boolean) userNode.getProperty(USER_SUSPENDED);
        String username = (String) userNode.getProperty(USER_NAME);
        boolean requirePasswordChange = (boolean) userNode.getProperty(USER_EXPIRED, false);
        Credential credential = null;
        var maybeCredentials = userNode.getProperty(USER_CREDENTIALS, null);
        if (maybeCredentials instanceof String rawCredentials) {
            try {
                credential = SystemGraphCredential.deserialize(rawCredentials, secureHasher);
            } catch (FormatException e) {
                securityLog.debug(String.format("Wrong format of credentials for user %s.", username));
                return null;
            }
        }

        try (ResourceIterable<Relationship> authRels = userNode.getRelationships(Direction.OUTGOING, HAS_AUTH)) {
            var auths = new HashSet<User.Auth>();
            if (!authRels.iterator().hasNext()) {
                // old users have no auth object associated with them
                if (credential != null) {
                    auths.add(new User.Auth(NATIVE_AUTH, userId));
                }
            }
            for (Relationship rel : authRels) {
                Node authNode = rel.getEndNode();
                String authProvider = (String) authNode.getProperty(AUTH_PROVIDER);
                var id = (String) authNode.getProperty(AUTH_ID);
                var auth = new User.Auth(authProvider, id);
                auths.add(auth);
            }
            User user = new User(
                    username,
                    userId,
                    new User.SensitiveCredential(credential),
                    requirePasswordChange,
                    suspended,
                    auths);
            securityLog.debug(String.format("Found user: %s", user));
            return user;
        }
    }

    public User getUserByAuth(String provider, String authId) {
        securityLog.debug(String.format("Looking up user with auth provider: %s, auth id: %s", provider, authId));
        try (var tx = systemSupplier.get().beginTx()) {
            try (ResourceIterator<Node> authNodeIterator =
                    tx.findNodes(AUTH_LABEL, AUTH_ID, authId, AUTH_PROVIDER, provider)) {
                if (authNodeIterator.hasNext()) {
                    Node authNode = authNodeIterator.next();
                    Relationship hasAuth = authNode.getSingleRelationship(HAS_AUTH, Direction.INCOMING);
                    if (hasAuth == null) {
                        securityLog.debug(
                                String.format("No user found with auth provider: %s, auth id: %s", provider, authId));
                        return null;
                    }
                    return getUser(hasAuth.getStartNode());
                } else {
                    securityLog.debug(String.format("No auth for auth provider: %s, auth id: %s", provider, authId));
                    return null;
                }
            }
        }
    }

    public String getAuthId(String provider, String username) {
        securityLog.debug(String.format("Looking up '%s' auth for user '%s'", provider, username));
        User user = getUserByName(username);
        String id = null;
        if (user != null) {
            id = user.auth().stream()
                    .filter(auth -> auth.provider().equals(provider))
                    .map(User.Auth::id)
                    .findFirst()
                    .orElse(null);
            if (id == null) {
                securityLog.debug(String.format("'%s' auth not found for user '%s'", provider, username));
            }
        }
        return id;
    }

    public boolean hasExternalAuth(String username) {
        securityLog.debug(String.format("Checking whether user '%s' has an external auth", username));
        User user = getUserByName(username);
        if (user == null) {
            return false;
        }
        boolean externalAuth = user.auth().stream().anyMatch(a -> !a.provider().equals(NATIVE_AUTH));
        if (externalAuth) {
            securityLog.debug(String.format("External auth found for user '%s'", username));
        } else {
            securityLog.debug(String.format("No external auth found for user '%s'", username));
        }
        return externalAuth;
    }
}
