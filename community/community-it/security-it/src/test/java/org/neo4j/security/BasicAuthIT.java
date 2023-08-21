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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@DbmsExtension(configurationCallback = "configure")
class BasicAuthIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private DbmsController dbmsController;

    @Inject
    private AuthManager authManager;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.auth_enabled, false);
    }

    @Test
    void shouldCreateUserWithAuthDisabled() throws Exception {
        // GIVEN
        var systemDatabase = managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDatabase.beginTx()) {
            // WHEN
            tx.execute("CREATE USER foo SET PASSWORD 'barpassword'").close();
            tx.commit();
        }
        dbmsController.restartDbms(builder -> builder.setConfig(GraphDatabaseSettings.auth_enabled, true));

        // THEN
        LoginContext loginContext = authManager.login(AuthToken.newBasicAuthToken("foo", "wrong"), EMBEDDED_CONNECTION);
        assertThat(loginContext.subject().getAuthenticationResult()).isEqualTo(AuthenticationResult.FAILURE);
        loginContext = authManager.login(AuthToken.newBasicAuthToken("foo", "barpassword"), EMBEDDED_CONNECTION);
        assertThat(loginContext.subject().getAuthenticationResult())
                .isEqualTo(AuthenticationResult.PASSWORD_CHANGE_REQUIRED);
    }

    @Test
    void shouldFailImpersonate() throws Exception {
        // GIVEN
        var systemDatabase = managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDatabase.beginTx()) {
            tx.execute("CREATE USER foo SET PASSWORD 'barpassword' CHANGE NOT REQUIRED")
                    .close();
            tx.execute("CREATE USER baz SET PASSWORD 'barpassword'").close();
            tx.commit();
        }
        dbmsController.restartDbms(builder -> builder.setConfig(GraphDatabaseSettings.auth_enabled, true));

        // WHEN...THEN
        LoginContext loginContext =
                authManager.login(AuthToken.newBasicAuthToken("foo", "barpassword"), EMBEDDED_CONNECTION);
        assertThat(loginContext.subject().getAuthenticationResult()).isEqualTo(AuthenticationResult.SUCCESS);
        assertThatThrownBy(() -> authManager.impersonate(loginContext, "baz"))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Impersonation is not supported in community edition.");
    }

    @Test
    void shouldFailImpersonateWithAuthDisabled() throws Exception {
        // GIVEN
        var systemDatabase = managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDatabase.beginTx()) {
            tx.execute("CREATE USER foo SET PASSWORD 'barpassword'").close();
            tx.commit();
        }

        // WHEN...THEN
        LoginContext loginContext = authManager.login(Collections.emptyMap(), EMBEDDED_CONNECTION);
        assertThat(loginContext.subject().getAuthenticationResult()).isEqualTo(AuthenticationResult.SUCCESS);
        assertThatThrownBy(() -> authManager.impersonate(loginContext, "foo"))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Impersonation is not supported with auth disabled.");
    }
}
