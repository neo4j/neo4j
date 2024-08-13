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
package org.neo4j.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH;
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

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;
import org.neo4j.server.security.systemgraph.SecurityGraphHelper;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension()
public class SecurityGraphHelperTest {
    @Inject
    private DatabaseManagementService dbms;

    private SecurityGraphHelper securityGraphHelper;
    private AbstractSecurityLog securityLog;
    GraphDatabaseService system;

    @BeforeEach
    void setUp() {
        system = dbms.database(SYSTEM_DATABASE_NAME);
        securityLog = mock(AbstractSecurityLog.class);
        securityGraphHelper =
                new SecurityGraphHelper(Suppliers.lazySingleton(() -> system), new SecureHasher(), securityLog);
    }

    @AfterEach
    void tearDown() {
        try (var tx = system.beginTx()) {
            List<String> users = tx.execute("SHOW USERS YIELD user").map(m -> (String) m.get("user")).stream()
                    .toList();
            for (String user : users) {
                tx.execute("DROP USER " + user);
            }
            tx.commit();
        }
    }

    @Test
    void getAuthShouldFindAuthObjectForUserNameAndProvider() {
        // GIVEN
        createUser(new User("alice", "aliceId", null, false, false, Set.of(new User.Auth("provider1", "sub123"))));

        // WHEN
        String result = securityGraphHelper.getAuthId("provider1", "alice");

        // THEN
        assertThat(result).isEqualTo("sub123");
        verify(securityLog).debug("Looking up 'provider1' auth for user 'alice'");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=aliceId, credential=null, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=provider1, id=sub123]]]");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getAuthShouldFindNativeAuth() {
        // GIVEN
        createUser(new User("alice", "uuid", null, false, false, Set.of(new User.Auth(NATIVE_AUTH, "aliceId"))));

        // WHEN
        String result = securityGraphHelper.getAuthId(NATIVE_AUTH, "alice");

        // THEN
        assertThat(result).isEqualTo("aliceId");
        verify(securityLog).debug("Looking up 'native' auth for user 'alice'");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=uuid, credential=null, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=native, id=aliceId]]]");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getAuthShouldReturnNullAndLogWhenUserDoesNotExist() {
        // WHEN
        String result = securityGraphHelper.getAuthId("provider1", "alice");

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up 'provider1' auth for user 'alice'");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog).debug("User 'alice' not found");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getAuthShouldReturnNullAndLogWhenAuthDoesNotExists() {
        // GIVEN
        createUser(new User("alice", "userId", null, false, false));

        // WHEN
        String result = securityGraphHelper.getAuthId("provider1", "alice");

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up 'provider1' auth for user 'alice'");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=null, passwordChangeRequired=false, suspended=false, auth=[]]");
        verify(securityLog).debug("'provider1' auth not found for user 'alice'");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void hasExternalAuthShouldReturnTrueWhenUserHasExternalAuth() {
        // GIVEN
        createUser(new User("alice", "aliceId", null, false, false, Set.of(new User.Auth("provider1", "sub123"))));

        // WHEN
        boolean result = securityGraphHelper.hasExternalAuth("alice");

        // THEN
        assertThat(result).isTrue();

        verify(securityLog).debug("Checking whether user 'alice' has an external auth");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=aliceId, credential=null, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=provider1, id=sub123]]]");
        verify(securityLog).debug("External auth found for user 'alice'");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void hasExternalAuthObjectShouldReturnFalseWhenUserHasNativeAuthObject() {
        // GIVEN
        createUser(new User("alice", "aliceId", null, false, false, Set.of(new User.Auth(NATIVE_AUTH, "aliceId"))));

        // WHEN
        boolean result = securityGraphHelper.hasExternalAuth("alice");

        // THEN
        assertThat(result).isFalse();
        verify(securityLog).debug("Checking whether user 'alice' has an external auth");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=aliceId, credential=null, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=native, id=aliceId]]]");
        verify(securityLog).debug("No external auth found for user 'alice'");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void hasAuthObjectShouldReturnFalseWhenUserDoesNotExist() {
        // WHEN
        boolean result = securityGraphHelper.hasExternalAuth("alice");

        // THEN
        assertThat(result).isFalse();
        verify(securityLog).debug("Checking whether user 'alice' has an external auth");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog).debug("User 'alice' not found");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void hasAuthObjectShouldReturnFalseWhenNoAuthObjectExists() {
        // GIVEN
        createUser(new User("alice", "userId", null, false, false));

        // WHEN
        boolean result = securityGraphHelper.hasExternalAuth("alice");

        // THEN
        assertThat(result).isFalse();
        verify(securityLog).debug("Checking whether user 'alice' has an external auth");
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=null, passwordChangeRequired=false, suspended=false, auth=[]]");
        verify(securityLog).debug("No external auth found for user 'alice'");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByAuthShouldReturnUser() {
        createUser(new User("alice", "userId", null, false, false, Set.of(new User.Auth("provider1", "sub123"))));
        User result = securityGraphHelper.getUserByAuth("provider1", "sub123");
        assertThat(result.name()).isEqualTo("alice");
        assertThat(result.id()).isEqualTo("userId");
        assertThat(result.passwordChangeRequired()).isFalse();
        assertThat(result.suspended()).isFalse();
        assertThat(result.auth()).isEqualTo(Set.of(new User.Auth("provider1", "sub123")));
        verify(securityLog).debug("Looking up user with auth provider: provider1, auth id: sub123");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=null, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=provider1, id=sub123]]]");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByAuthShouldReturnNullWhenAuthDoesNotExist() {
        createUser(new User("alice", "userId", null, false, false, Set.of(new User.Auth("provider1", "sub456"))));
        User result = securityGraphHelper.getUserByAuth("provider1", "sub123");
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up user with auth provider: provider1, auth id: sub123");
        verify(securityLog).debug("No auth for auth provider: provider1, auth id: sub123");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnNullUserIdIsNull() {
        // WHEN
        User result = securityGraphHelper.getUserById(null);

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up user with id 'null'");
        verify(securityLog).debug("Cannot look up user with id = null");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnUserWithNativeAuthForLegacyUserWhenCredentialsSet() {
        // GIVEN
        var credential = SystemGraphCredential.createCredentialForPassword(UTF8.encode("password"), new SecureHasher());
        createUser(new User("alice", "userId", credential, false, false));

        // WHEN
        User result = securityGraphHelper.getUserById("userId");

        // THEN
        assertThat(result.id()).isEqualTo("userId");
        assertThat(result.name()).isEqualTo("alice");
        assertThat(result.auth()).isEqualTo(Set.of(new User.Auth(NATIVE_AUTH, "userId")));
        verify(securityLog).debug("Looking up user with id 'userId'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=*****, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=native, id=userId]]]");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnUserWithoutNativeAuthForLegacyUserWhenCredentialsNotSet() {
        // GIVEN
        createUser(new User("alice", "userId", null, false, false));

        // WHEN
        User result = securityGraphHelper.getUserById("userId");

        // THEN
        assertThat(result.id()).isEqualTo("userId");
        assertThat(result.name()).isEqualTo("alice");
        assertThat(result.auth()).isEqualTo(Set.of());
        verify(securityLog).debug("Looking up user with id 'userId'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=null, passwordChangeRequired=false, suspended=false, auth=[]]");
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnNullWhenUserDoesNotExist() {
        // WHEN
        User result = securityGraphHelper.getUserById("userId");

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up user with id 'userId'");
        verify(securityLog).debug("User with id 'userId' not found");
        verifyNoMoreInteractions(securityLog);
    }

    void createUser(User user) {
        try (var tx = system.beginTx()) {
            Node userNode = tx.createNode(USER_LABEL);
            userNode.setProperty(USER_NAME, user.name());
            userNode.setProperty(USER_ID, user.id());
            if (user.credential() != null && user.credential().value() != null) {
                userNode.setProperty(USER_CREDENTIALS, user.credential().value().serialize());
                userNode.setProperty(USER_EXPIRED, user.passwordChangeRequired());
            }
            userNode.setProperty(USER_SUSPENDED, user.suspended());
            for (User.Auth auth : user.auth()) {
                Node authNode = tx.createNode(AUTH_LABEL);
                authNode.setProperty(AUTH_PROVIDER, auth.provider());
                authNode.setProperty(AUTH_ID, auth.id());
                userNode.createRelationshipTo(authNode, HAS_AUTH);
            }
            tx.commit();
        }
    }
}
