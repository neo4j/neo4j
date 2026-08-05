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
package org.neo4j.server.security.systemgraph.versions;

import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_LABEL;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_PROVIDER_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.HAS_AUTH_TYPE;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH;

import java.util.List;
import java.util.UUID;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersion;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.FormatException;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;
import org.neo4j.server.security.systemgraph.ShowUsersOutput;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion;
import org.neo4j.string.UTF8;

public abstract class KnownCommunitySecurityComponentVersion extends KnownSystemComponentVersion {
    private final SecureHasher secureHasher = new SecureHasher();
    private final AbstractSecurityLog securityLog;

    KnownCommunitySecurityComponentVersion(
            ComponentVersion componentVersion, Log debugLog, AbstractSecurityLog securityLog) {
        super(componentVersion, debugLog);
        this.securityLog = securityLog;
    }

    public abstract void setupUsers(Transaction tx, Config config) throws Exception;

    public void addUser(
            Transaction tx,
            String username,
            Credential credentials,
            boolean passwordChangeRequired,
            boolean suspended) {
        // NOTE: If username already exists we will violate a constraint
        securityLog.info(
                "CREATE USER %s PASSWORD ****** CHANGE %s%s",
                username,
                passwordChangeRequired ? "REQUIRED" : "NOT REQUIRED",
                suspended ? " SET STATUS SUSPENDED" : "");
        Node node = tx.createNode(USER_LABEL);
        node.setProperty(USER_NAME_PROPERTY, username);
        node.setProperty(USER_CREDENTIALS_PROPERTY, credentials.serialize());
        node.setProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, passwordChangeRequired);
        node.setProperty(USER_SUSPENDED_PROPERTY, suspended);
        node.setProperty(USER_ID_PROPERTY, UUID.randomUUID().toString());
        if (version >= UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_521.getVersion()) {
            addAuthObject(tx, node);
        }
    }

    public abstract void updateInitialUserPassword(Transaction tx) throws Exception;

    void updateInitialUserPassword(Transaction tx, User initialUser) throws FormatException {
        // The set-initial-password command should only take effect if the only existing user is the default user with
        // the default password.
        List<Node> users = Iterators.asList(tx.findNodes(USER_LABEL));
        if (users.isEmpty()) {
            debugLog.warn(String.format(
                    "Unable to update missing initial user password from `auth.ini` file: %s", initialUser.name()));
        } else if (users.size() == 1) {
            Node user = users.get(0);
            if (user.getProperty(USER_NAME_PROPERTY).equals(INITIAL_USER_NAME)) {
                SystemGraphCredential currentCredentials = SystemGraphCredential.deserialize(
                        user.getProperty(USER_CREDENTIALS_PROPERTY).toString(), secureHasher);
                if (currentCredentials.matchesPassword(UTF8.encode(INITIAL_PASSWORD))) {
                    debugLog.info(String.format(
                            "Updating initial user password from `auth.ini` file: %s", initialUser.name()));
                    user.setProperty(
                            USER_CREDENTIALS_PROPERTY,
                            initialUser.credential().value().serialize());
                    user.setProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, initialUser.passwordChangeRequired());
                }
            }
        } else {
            debugLog.warn("Unable to update initial user password from `auth.ini` file: multiple users in the DBMS");
        }
    }

    public void addAuthObjects(Transaction tx) {
        try (ResourceIterator<Node> nodes = tx.findNodes(USER_LABEL)) {
            while (nodes.hasNext()) {
                Node user = nodes.next();
                if (user.hasProperty(USER_CREDENTIALS_PROPERTY) && !user.hasRelationship(HAS_AUTH_TYPE)) {
                    addAuthObject(tx, user);
                }
            }
        }
    }

    private void addAuthObject(Transaction tx, Node user) {
        String userId = (String) user.getProperty(USER_ID_PROPERTY);
        Node authNode = tx.createNode(AUTH_LABEL);
        authNode.setProperty(AUTH_PROVIDER_PROPERTY, NATIVE_AUTH);
        authNode.setProperty(AUTH_ID_PROPERTY, userId);
        user.createRelationshipTo(authNode, HAS_AUTH_TYPE);
    }

    /**
     * Upgrade the security graph to this version.
     * This method recursively calls older versions and performs the upgrades in steps.
     *
     * @param tx open transaction to perform the upgrade in
     * @param fromVersion the detected version, upgrade will be performed rolling from this
     */
    public abstract void upgradeSecurityGraph(Transaction tx, int fromVersion) throws Exception;

    /**
     * Upgrade the security graph schema to this version.
     * This method recursively calls older versions and performs the upgrades in steps.
     *
     * @param tx open transaction to perform the upgrade in
     * @param fromVersion the detected version, upgrade will be performed rolling from this
     */
    public abstract void upgradeSecurityGraphSchema(Transaction tx, int fromVersion) throws Exception;

    public boolean requiresAuthObject() {
        return true;
    }

    public abstract ShowUsersOutput showUsers(
            Transaction tx, boolean withAuth, boolean allowedToSeeTags, boolean asCommands, boolean enterprise);
}
