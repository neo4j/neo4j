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
package org.neo4j.server.security.auth;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.function.Suppliers.lazySingleton;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.nio.file.Path;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.time.Clocks;

public class CommunitySecurityModule extends SecurityModule {
    private final InternalLogProvider debugLogProvider;
    private final Config config;
    private final Dependencies globalDependencies;
    private BasicSystemGraphRealm authManager;

    public CommunitySecurityModule(LogService logService, Config config, Dependencies globalDependencies) {
        this.debugLogProvider = logService.getInternalLogProvider();
        this.config = config;
        this.globalDependencies = globalDependencies;
    }

    @Override
    public void setup() {
        Suppliers.Lazy<GraphDatabaseService> systemSupplier = lazySingleton(() -> {
            DatabaseContextProvider<?> databaseContextProvider =
                    globalDependencies.resolveDependency(DatabaseContextProvider.class);
            return databaseContextProvider
                    .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                    .orElseThrow(
                            () -> new RuntimeException("No database called `" + SYSTEM_DATABASE_NAME + "` was found."))
                    .databaseFacade();
        });

        authManager = new BasicSystemGraphRealm(
                new SystemGraphRealmHelper(systemSupplier, new SecureHasher()), createAuthenticationStrategy(config));

        registerProcedure(
                globalDependencies.resolveDependency(GlobalProcedures.class),
                debugLogProvider.getLog(getClass()),
                AuthProcedures.class);
    }

    @Override
    public AuthManager authManager() {
        return authManager;
    }

    @Override
    public AuthManager inClusterAuthManager() {
        return null;
    }

    @Override
    public AuthManager loopbackAuthManager() {
        return null;
    }

    private static final String INITIAL_USER_STORE_FILENAME = "auth.ini";

    private static FileUserRepository getInitialUserRepository(
            Config config,
            InternalLogProvider logProvider,
            FileSystemAbstraction fileSystem,
            MemoryTracker memoryTracker) {
        return new FileUserRepository(fileSystem, getInitialUserRepositoryFile(config), logProvider, memoryTracker);
    }

    public static Path getInitialUserRepositoryFile(Config config) {
        Path authStoreDir = config.get(GraphDatabaseInternalSettings.auth_store_directory);
        return authStoreDir.resolve(INITIAL_USER_STORE_FILENAME);
    }

    public static UserSecurityGraphComponent createSecurityComponent(
            Config config,
            FileSystemAbstraction fileSystem,
            InternalLogProvider logProvider,
            AbstractSecurityLog securityLog,
            MemoryTracker memoryTracker) {
        UserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository(config, logProvider, fileSystem, memoryTracker);

        return new UserSecurityGraphComponent(initialUserRepository, config, logProvider, securityLog);
    }

    public static AuthenticationStrategy createAuthenticationStrategy(Config config) {
        return new RateLimitedAuthenticationStrategy(Clocks.systemClock(), config);
    }
}
