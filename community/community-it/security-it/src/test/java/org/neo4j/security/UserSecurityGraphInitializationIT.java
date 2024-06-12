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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFails;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.createUser;

import java.time.Clock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * This class tests initialization of a fresh system database.
 */
@TestDirectoryExtension
class UserSecurityGraphInitializationIT {
    private BasicSystemGraphRealmTestHelper.TestDatabaseContextProvider dbManager;
    private SystemGraphRealmHelper realmHelper;

    @SuppressWarnings("unused")
    @Inject
    private TestDirectory testDirectory;

    private UserRepository initialPassword;
    private SystemGraphInitializer systemGraphInitializer;

    @BeforeEach
    void setUp() {
        dbManager = new BasicSystemGraphRealmTestHelper.TestDatabaseContextProvider(testDirectory);
        SecureHasher secureHasher = new SecureHasher();
        realmHelper = new SystemGraphRealmHelper(SystemGraphRealmHelper.makeSystemSupplier(dbManager), secureHasher);
        initialPassword = new InMemoryUserRepository();
    }

    @AfterEach
    void tearDown() {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldCreateDefaultUserIfNoneExist() throws Throwable {
        startSystemGraphRealm();
        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true);
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable {
        initialPassword.create(createUser(INITIAL_USER_NAME, "123", false));
        startSystemGraphRealm();
        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, "123");
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable {
        // Given
        startSystemGraphRealm();

        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true);

        initialPassword.create(createUser(INITIAL_USER_NAME, "abc", false));

        // When
        systemGraphInitializer.start();

        // Then
        assertAuthenticationFails(realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD);
        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, "abc");
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable {
        // Given
        startSystemGraphRealm();
        initialPassword.create(createUser(INITIAL_USER_NAME, "neo4j2", false));
        systemGraphInitializer.start();

        // When
        initialPassword.clear();
        initialPassword.create(createUser(INITIAL_USER_NAME, "abc", false));
        systemGraphInitializer.start();

        // Then
        assertAuthenticationFails(realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD);
        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, "neo4j2");
        assertAuthenticationFails(realmHelper, INITIAL_USER_NAME, "abc");
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordWhenOtherUsersExist() throws Throwable {
        // Given
        startSystemGraphRealm();
        try (var tx = dbManager.testSystemDb.beginTx()) {
            tx.execute("CREATE USER Alice SET PASSWORD 'password'");
            tx.commit();
        }
        // When
        initialPassword.create(createUser(INITIAL_USER_NAME, "neo4j2password", false));
        systemGraphInitializer.start();

        // Then
        assertAuthenticationSucceeds(realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true);
        assertAuthenticationFails(realmHelper, INITIAL_USER_NAME, "neo4j2password");
    }

    @Test
    void shouldNotReCreateInitialUser() throws Throwable {
        // Given
        startSystemGraphRealm();
        try (var tx = dbManager.testSystemDb.beginTx()) {
            tx.execute(String.format("DROP USER %s", INITIAL_USER_NAME));
            tx.commit();
        }
        // When
        initialPassword.create(createUser(INITIAL_USER_NAME, "neo4j2", false));
        systemGraphInitializer.start();

        // Then
        assertThatThrownBy(() -> realmHelper.getUser(INITIAL_USER_NAME))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage(String.format("User '%s' does not exist.", INITIAL_USER_NAME));
    }

    private void startSystemGraphRealm() throws Exception {
        Config config = Config.defaults(
                GraphDatabaseInternalSettings.auth_store_directory, testDirectory.directory("data/dbms"));

        var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
        systemGraphComponentsBuilder.register(new DefaultSystemGraphComponent(config, Clock.systemUTC()));
        systemGraphComponentsBuilder.register(new UserSecurityGraphComponent(
                initialPassword, config, NullLogProvider.getInstance(), CommunitySecurityLog.NULL_LOG));

        var systemGraphSupplier = SystemGraphRealmHelper.makeSystemSupplier(dbManager);
        systemGraphInitializer =
                new DefaultSystemGraphInitializer(systemGraphSupplier, systemGraphComponentsBuilder.build());
        systemGraphInitializer.start();
    }
}
