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

import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.FormatException;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;

public class SecurityGraphHelper {
    public static final String NATIVE_AUTH = AuthToken.NATIVE_REALM;

    protected final AbstractSecurityLog securityLog;
    protected final Supplier<GraphDatabaseService> systemSupplier;
    protected final SecureHasher secureHasher;

    public SecurityGraphHelper(
            Supplier<GraphDatabaseService> systemSupplier, SecureHasher secureHasher, AbstractSecurityLog securityLog) {
        this.systemSupplier = systemSupplier;
        this.secureHasher = secureHasher;
        this.securityLog = securityLog;
    }

    public GraphDatabaseService getSystemDb() {
        return systemSupplier.get();
    }

    /**
     * Lookup a user information from username, returns null if the user does not exist.
     *
     * @param username username
     * @return user record containing user and auth information
     */
    public User getUserByName(String username) {
        securityLog.debug("Looking up user '%s'", username);
        if (username == null) {
            securityLog.debug("Cannot look up user 'null'");
            return null;
        }
        try (var tx = systemSupplier.get().beginTx()) {
            Node userNode = tx.findNode(USER_LABEL, USER_NAME_PROPERTY, username);
            if (userNode == null) {
                securityLog.debug("User '%s' not found", username);
                return null;
            }
            return getUser(userNode);
        } catch (NotFoundException n) {
            // Can occur if the user was dropped by another thread after the null check.
            securityLog.debug("User '%s' not found", username);
            return null;
        }
    }

    protected User getUser(Node userNode) {
        var userId = (String) userNode.getProperty(USER_ID_PROPERTY);
        String username = (String) userNode.getProperty(USER_NAME_PROPERTY);
        boolean requirePasswordChange = (boolean) userNode.getProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, false);
        Credential credential = null;
        var maybeCredentials = userNode.getProperty(USER_CREDENTIALS_PROPERTY, null);
        if (maybeCredentials instanceof String rawCredentials) {
            try {
                credential = SystemGraphCredential.deserialize(rawCredentials, secureHasher);
            } catch (FormatException e) {
                securityLog.debug("Wrong format of credentials for user %s.", username);
                return null;
            }
        }

        User user = new User(
                username,
                userId,
                new User.SensitiveCredential(credential),
                requirePasswordChange,
                false,
                credential == null ? Collections.emptySet() : Set.of(new User.Auth(NATIVE_AUTH, userId)));
        securityLog.debug("Found user: %s", user);
        return user;
    }

    public User getUserByAuth(String provider, String authId) {
        return null;
    }

    public String getAuthId(String provider, String username) {
        return null;
    }

    public boolean hasExternalAuth(String username) {
        return false;
    }
}
