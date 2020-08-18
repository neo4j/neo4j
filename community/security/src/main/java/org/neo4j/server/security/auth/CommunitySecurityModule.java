/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.auth;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.time.Clocks;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class CommunitySecurityModule extends SecurityModule
{
    private final LogProvider logProvider;
    private final Config config;
    private final Dependencies globalDependencies;
    private BasicSystemGraphRealm authManager;

    public CommunitySecurityModule(
            LogService logService,
            Config config,
            Dependencies globalDependencies )
    {
        this.logProvider = logService.getUserLogProvider();
        this.config = config;
        this.globalDependencies = globalDependencies;
    }

    @Override
    public void setup()
    {
        Supplier<GraphDatabaseService> systemSupplier = () ->
        {
            DatabaseManager<?> databaseManager = globalDependencies.resolveDependency( DatabaseManager.class );
            return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                    () -> new RuntimeException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        };

        authManager = new BasicSystemGraphRealm(
                new SystemGraphRealmHelper( systemSupplier, new SecureHasher() ),
                createAuthenticationStrategy( config )
        );

        registerProcedure( globalDependencies.resolveDependency( GlobalProcedures.class ), logProvider.getLog( getClass() ), AuthProcedures.class, null );
    }

    @Override
    public AuthManager authManager()
    {
        return authManager;
    }

    @Override
    public AuthManager inClusterAuthManager()
    {
        return null;
    }

    private static final String USER_STORE_FILENAME = "auth";
    private static final String INITIAL_USER_STORE_FILENAME = "auth.ini";

    public static FileUserRepository getUserRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getUserRepositoryFile( config ), logProvider );
    }

    private static FileUserRepository getInitialUserRepository( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getInitialUserRepositoryFile( config ), logProvider );
    }

    public static Path getUserRepositoryFile( Config config )
    {
        // Because it contains sensitive information there is a legacy setting to configure
        // the location of the user store file that we still respect
        Path authStore = config.get( GraphDatabaseInternalSettings.auth_store );
        if ( Files.isRegularFile( authStore ) )
        {
            return authStore;
        }
        return getUserRepositoryFile( config, USER_STORE_FILENAME );
    }

    public static Path getInitialUserRepositoryFile( Config config )
    {
        return getUserRepositoryFile( config, INITIAL_USER_STORE_FILENAME );
    }

    private static Path getUserRepositoryFile( Config config, String fileName )
    {
        // Resolve auth store file names
        Path authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory );
        return authStoreDir.resolve( fileName );
    }

    public static UserSecurityGraphComponent createSecurityComponent( Log log, Config config, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        UserRepository migrationUserRepository = CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        UserRepository initialUserRepository = CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );

        return new UserSecurityGraphComponent( log, migrationUserRepository, initialUserRepository, config );
    }

    public static AuthenticationStrategy createAuthenticationStrategy( Config config )
    {
        return new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config );
    }
}
