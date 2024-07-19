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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Optional;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.systemgraph.SecurityGraphHelper;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.utils.TestDirectory;

public class BasicSystemGraphRealmTestHelper {
    public static class TestDatabaseContextProvider extends LifecycleAdapter
            implements DatabaseContextProvider<StandaloneDatabaseContext> {
        protected GraphDatabaseFacade testSystemDb;
        protected final DatabaseManagementService managementService;
        private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

        protected TestDatabaseContextProvider(TestDirectory testDir) {
            managementService = createManagementService(testDir);
            testSystemDb = (GraphDatabaseFacade) managementService.database(SYSTEM_DATABASE_NAME);
        }

        protected DatabaseManagementService createManagementService(TestDirectory testDir) {
            return new TestDatabaseManagementServiceBuilder(testDir.homePath())
                    .impermanent()
                    .noOpSystemGraphInitializer()
                    .setConfig(GraphDatabaseSettings.auth_enabled, false)
                    .build();
        }

        public DatabaseManagementService getManagementService() {
            return managementService;
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext(NamedDatabaseId namedDatabaseId) {
            if (namedDatabaseId.isSystemDatabase()) {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency(Database.class);
                return Optional.of(new StandaloneDatabaseContext(database));
            }
            return Optional.empty();
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext(String databaseName) {
            return databaseIdRepository().getByName(databaseName).flatMap(this::getDatabaseContext);
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext(DatabaseId databaseId) {
            return databaseIdRepository().getById(databaseId).flatMap(this::getDatabaseContext);
        }

        @Override
        public DatabaseIdRepository databaseIdRepository() {
            return databaseIdRepository;
        }

        @Override
        public NavigableMap<NamedDatabaseId, StandaloneDatabaseContext> registeredDatabases() {
            return Collections.emptyNavigableMap();
        }
    }

    public static void assertAuthenticationSucceeds(SecurityGraphHelper realmHelper, String username, String password) {
        assertAuthenticationSucceeds(realmHelper, username, password, false);
    }

    public static void assertAuthenticationSucceeds(
            SecurityGraphHelper realmHelper, String username, String password, boolean changeRequired) {
        var user = realmHelper.getUserByName(username);
        assertTrue(user.credential().value().matchesPassword(password(password)));
        assertThat(user.passwordChangeRequired())
                .withFailMessage(
                        "Expected change required to be %s, but was %s", changeRequired, user.passwordChangeRequired())
                .isEqualTo(changeRequired);
    }

    public static void assertAuthenticationFails(SecurityGraphHelper realmHelper, String username, String password) {
        var user = realmHelper.getUserByName(username);
        assertFalse(user.credential().value().matchesPassword(password(password)));
    }

    public static User createUser(String userName, String password, boolean pwdChangeRequired) {
        return new User(userName, null, credentialFor(password), pwdChangeRequired, false);
    }
}
