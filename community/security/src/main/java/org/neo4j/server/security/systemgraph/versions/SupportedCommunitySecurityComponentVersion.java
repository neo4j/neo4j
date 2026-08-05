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
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.AUTH_PROVIDER_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.HAS_AUTH_TYPE;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.HAS_ROLE_TYPE;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.ROLE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_HOME_DB_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_TAGS_PROPERTY;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH;
import static org.neo4j.util.Stringifier.backtick;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.ShowUsersOutput;
import org.neo4j.string.UTF8;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

public abstract class SupportedCommunitySecurityComponentVersion extends KnownCommunitySecurityComponentVersion {
    private final UserRepository userRepository;
    private final SecureHasher secureHasher;

    SupportedCommunitySecurityComponentVersion(
            ComponentVersion componentVersion,
            UserRepository userRepository,
            Log debugLog,
            AbstractSecurityLog securityLog) {
        super(componentVersion, debugLog, securityLog);
        this.userRepository = userRepository;
        this.secureHasher = new SecureHasher();
    }

    @Override
    public void setupUsers(Transaction tx, Config config) throws Exception {
        if (Boolean.TRUE.equals(config.get(GraphDatabaseInternalSettings.create_default_user))) {
            addDefaultUser(tx);
        } else {
            debugLog.info(String.format(
                    "Not creating default user as per config setting: %s",
                    GraphDatabaseInternalSettings.create_default_user.name()));
        }
    }

    private void addDefaultUser(Transaction tx) throws Exception {
        Optional<User> initialUser = getInitialUser();
        if (initialUser.isPresent()) {
            User user = initialUser.get();
            debugLog.info(String.format("Setting up initial user from `auth.ini` file: %s", user.name()));
            addUser(tx, INITIAL_USER_NAME, user.credential().value(), user.passwordChangeRequired(), user.suspended());
        } else {
            SystemGraphCredential credential =
                    SystemGraphCredential.createCredentialForPassword(UTF8.encode(INITIAL_PASSWORD), secureHasher);
            debugLog.info(String.format("Setting up initial user from defaults: %s", INITIAL_USER_NAME));
            addUser(tx, INITIAL_USER_NAME, credential, true, false);
        }
    }

    @Override
    public void updateInitialUserPassword(Transaction tx) throws Exception {
        Optional<User> initialUser = getInitialUser();
        if (initialUser.isPresent()) {
            updateInitialUserPassword(tx, initialUser.get());
        } else {
            debugLog.debug("Not updating initial user password: No initial user found in `auth.ini`");
        }
    }

    private Optional<User> getInitialUser() throws Exception {
        userRepository.start();
        debugLog.debug("Opened `auth.ini` file to find the initial user");
        if (userRepository.numberOfUsers() == 0) {
            debugLog.debug("Not updating initial user password: No initial user found in `auth.ini`");
        }
        if (userRepository.numberOfUsers() == 1) {
            // In alignment with InternalFlatFileRealm we only allow the INITIAL_USER_NAME here for now
            // (This is what we get from the `set-initial-password` command
            User initialUser = userRepository.getUserByName(INITIAL_USER_NAME);
            if (initialUser == null) {
                String errorMessage = "Invalid `auth.ini` file: the user in the file is not named " + INITIAL_USER_NAME;
                debugLog.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            debugLog.debug("Valid `auth.ini` file: found initial user");
            return Optional.of(initialUser);
        } else if (userRepository.numberOfUsers() > 1) {
            String errorMessage = "Invalid `auth.ini` file: the file contains more than one user";
            debugLog.error(errorMessage);
            throw new IllegalStateException(errorMessage);
        }
        return Optional.empty();
    }

    @Override
    public ShowUsersOutput showUsers(
            Transaction tx, boolean withAuth, boolean allowedToSeeTags, boolean asCommands, boolean enterprise) {
        List<ShowUsersOutput.ShowUsersRow> usersRows = new ArrayList<>();
        try (ResourceIterator<Node> users = tx.findNodes(USER_LABEL)) {
            users.stream().forEach(userNode -> {
                try {
                    String username = (String) userNode.getProperty(USER_NAME_PROPERTY);
                    Object maybeChangeReq = userNode.getProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, null);
                    Boolean changeReq = maybeChangeReq != null ? (Boolean) maybeChangeReq : null;

                    Boolean suspended;
                    String home;
                    List<String> roles;
                    if (enterprise) {
                        suspended = (Boolean) userNode.getProperty(USER_SUSPENDED_PROPERTY, false);
                        Object maybeHome = userNode.getProperty(USER_HOME_DB_PROPERTY, null);
                        home = maybeHome != null ? (String) maybeHome : null;
                        // roles
                        try (ResourceIterable<Relationship> roleRels =
                                userNode.getRelationships(Direction.OUTGOING, HAS_ROLE_TYPE)) {
                            roles = Stream.concat(
                                            roleRels.stream()
                                                    .map(roleRel -> (String)
                                                            roleRel.getEndNode().getProperty(ROLE_NAME_PROPERTY))
                                                    .sorted(),
                                            Stream.of("PUBLIC"))
                                    .toList();
                        }
                    } else {
                        suspended = null;
                        home = null;
                        roles = null;
                    }

                    // tags
                    List<String> tags;
                    if (allowedToSeeTags) {
                        Object maybeTags = userNode.getProperty(USER_TAGS_PROPERTY, null);
                        if (maybeTags instanceof String[] rawTags) {
                            tags = Arrays.asList(rawTags);
                        } else {
                            tags = Collections.emptyList();
                        }
                    } else {
                        tags = null;
                    }

                    // WITH AUTH or AS COMMANDS
                    if (withAuth || asCommands) {
                        if (userNode.hasRelationship(Direction.OUTGOING, HAS_AUTH_TYPE)) {
                            // gather all auth data in a list
                            List<Auth> auths = new ArrayList<>();
                            try (ResourceIterable<Relationship> authRels =
                                    userNode.getRelationships(Direction.OUTGOING, HAS_AUTH_TYPE)) {
                                authRels.stream().forEach(authRel -> {
                                    Node authNode = authRel.getEndNode();
                                    String provider = (String) authNode.getProperty(AUTH_PROVIDER_PROPERTY);
                                    if (provider.equals(NATIVE_AUTH)) {
                                        var credentials = (String) userNode.getProperty(USER_CREDENTIALS_PROPERTY);
                                        auths.add(new NativeAuth(provider, credentials, changeReq));
                                    } else {
                                        String id = (String) authNode.getProperty(AUTH_ID_PROPERTY);
                                        auths.add(new ExternalAuth(provider, id));
                                    }
                                });
                            }

                            String command = null;
                            if (asCommands) {
                                String authString = auths.stream()
                                        .sorted(Comparator.comparing(Auth::provider))
                                        .map(Auth::asCommand)
                                        .collect(Collectors.joining());
                                command = asCommand(username, suspended, home, authString, tags);
                            }
                            if (withAuth) {
                                // SHOW USERS WITH AUTH [AS COMMANDS]
                                for (Auth auth : auths) {
                                    switch (auth) {
                                        case NativeAuth nativeAuth -> {
                                            Map<String, AnyValue> authMap = Map.of(
                                                    "password",
                                                    Values.stringValue("***"),
                                                    "changeRequired",
                                                    changeReq != null
                                                            ? Values.booleanValue(changeReq)
                                                            : Values.NO_VALUE);

                                            usersRows.add(new ShowUsersOutput.ShowUsersRow(
                                                    command,
                                                    username,
                                                    roles,
                                                    changeReq,
                                                    suspended,
                                                    home,
                                                    nativeAuth.provider,
                                                    authMap,
                                                    tags));
                                        }
                                        case ExternalAuth extAuth -> {
                                            Map<String, AnyValue> authMap =
                                                    Map.of("id", Values.stringValue(extAuth.id));
                                            usersRows.add(new ShowUsersOutput.ShowUsersRow(
                                                    command,
                                                    username,
                                                    roles,
                                                    changeReq,
                                                    suspended,
                                                    home,
                                                    extAuth.provider,
                                                    authMap,
                                                    tags));
                                        }
                                    }
                                }
                            } else {
                                // SHOW USERS AS COMMANDS
                                usersRows.add(new ShowUsersOutput.ShowUsersRow(
                                        command, username, roles, null, null, null, null, null, tags));
                            }
                        } else {
                            // no auth object found for the user
                            String command = null;
                            if (asCommands) {
                                command = asCommand(username, suspended, home, "", tags);
                            }
                            usersRows.add(new ShowUsersOutput.ShowUsersRow(
                                    command, username, roles, changeReq, suspended, home, null, null, tags));
                        }
                    } else {
                        usersRows.add(new ShowUsersOutput.ShowUsersRow(
                                null, username, roles, changeReq, suspended, home, null, null, tags));
                    }
                } catch (NotFoundException n) {
                    // user was deleted concurrently with reading properties from it,
                    // leave out of result and continue
                }
            });
        }
        return new ShowUsersOutput(usersRows);
    }

    private static String asCommand(String username, Boolean suspended, String home, String auth, List<String> tags) {
        var command = new StringBuilder();
        command.append("CREATE USER ").append(backtick(username));
        if (suspended != null && suspended) {
            command.append(" SET STATUS SUSPENDED");
        }
        if (home != null) {
            command.append(" SET HOME DATABASE ").append(home);
        }

        command.append(auth);

        if (tags != null && !tags.isEmpty()) {
            command.append(" SET TAGS [").append(String.join(",", tags)).append("]");
        }
        return command.toString();
    }

    private sealed interface Auth permits NativeAuth, ExternalAuth {
        String provider();

        String asCommand();
    }

    private record NativeAuth(String provider, String credentials, Boolean changeRequired) implements Auth {
        @Override
        public String asCommand() {
            String maskedCredentials = SystemGraphCredential.maskSerialized(credentials);
            var changeReq = changeRequired ? "" : " SET PASSWORD CHANGE NOT REQUIRED";
            return String.format(
                    " SET AUTH PROVIDER 'native' { SET ENCRYPTED PASSWORD '%s'%s }", maskedCredentials, changeReq);
        }
    }

    private record ExternalAuth(String provider, String id) implements Auth {
        @Override
        public String asCommand() {
            return String.format(" SET AUTH PROVIDER '%s' { SET ID '%s' }", provider, id);
        }
    }
}
