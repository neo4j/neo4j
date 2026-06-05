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
import static org.mockito.Mockito.when;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_ID_PROPERTY;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL;
import static org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH;

import java.util.Set;
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
class SecurityGraphHelperTest {
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
        when(securityLog.isDebugEnabled()).thenReturn(true);
    }

    @Test
    void getUserByNameShouldReturnNullUserIdIsNull() {
        // WHEN
        User result = securityGraphHelper.getUserByName(null);

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up user 'null'");
        verify(securityLog).debug("Cannot look up user 'null'");
        verify(securityLog).isDebugEnabled();
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnUserWithNativeAuthForLegacyUserWhenCredentialsSet() {
        // GIVEN
        var credential = SystemGraphCredential.createCredentialForPassword(UTF8.encode("password"), new SecureHasher());
        createUser(new User("alice", "userId", credential, false, false));

        // WHEN
        User result = securityGraphHelper.getUserByName("alice");

        // THEN
        assertThat(result.id()).isEqualTo("userId");
        assertThat(result.name()).isEqualTo("alice");
        assertThat(result.auth()).hasSameElementsAs(Set.of(new User.Auth(NATIVE_AUTH, "userId")));
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=*****, passwordChangeRequired=false, suspended=false, auth=[Auth[provider=native, id=userId]]]");
        verify(securityLog).isDebugEnabled();
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnUserWithoutNativeAuthForLegacyUserWhenCredentialsNotSet() {
        // GIVEN
        createUser(new User("alice", "userId", null, false, false));

        // WHEN
        User result = securityGraphHelper.getUserByName("alice");

        // THEN
        assertThat(result.id()).isEqualTo("userId");
        assertThat(result.name()).isEqualTo("alice");
        assertThat(result.auth()).hasSameElementsAs(Set.of());
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog)
                .debug(
                        "Found user: User[name=alice, id=userId, credential=null, passwordChangeRequired=false, suspended=false, auth=[]]");
        verify(securityLog).isDebugEnabled();
        verifyNoMoreInteractions(securityLog);
    }

    @Test
    void getUserByIdShouldReturnNullWhenUserDoesNotExist() {
        // WHEN
        User result = securityGraphHelper.getUserByName("alice");

        // THEN
        assertThat(result).isNull();
        verify(securityLog).debug("Looking up user 'alice'");
        verify(securityLog).debug("User 'alice' not found");
        verify(securityLog).isDebugEnabled();
        verifyNoMoreInteractions(securityLog);
    }

    void createUser(User user) {
        try (var tx = system.beginTx()) {
            Node userNode = tx.createNode(USER_LABEL);
            userNode.setProperty(USER_NAME_PROPERTY, user.name());
            userNode.setProperty(USER_ID_PROPERTY, user.id());
            if (user.credential() != null && user.credential().value() != null) {
                userNode.setProperty(
                        USER_CREDENTIALS_PROPERTY, user.credential().value().serialize());
                userNode.setProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, user.passwordChangeRequired());
            }
            tx.commit();
        }
    }
}
