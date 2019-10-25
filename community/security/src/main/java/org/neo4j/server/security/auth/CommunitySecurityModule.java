/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.UserSecurityGraphInitializer;
import org.neo4j.time.Clocks;

@ServiceProvider
public class CommunitySecurityModule extends SecurityModule
{
    private BasicSystemGraphRealm authManager;
    private SystemGraphInitializer systemGraphInitializer;
    private DatabaseManager<?> databaseManager;

    @Override
    public String getName()
    {
        return "community-security-module";
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        org.neo4j.collection.Dependencies platformDependencies = (org.neo4j.collection.Dependencies) dependencies.dependencySatisfier();
        this.databaseManager = platformDependencies.resolveDependency( DatabaseManager.class );
        this.systemGraphInitializer = platformDependencies.resolveDependency( SystemGraphInitializer.class );

        Config config = dependencies.config();
        GlobalProcedures globalProcedures = dependencies.procedures();
        LogProvider logProvider = dependencies.logService().getUserLogProvider();
        FileSystemAbstraction fileSystem = dependencies.fileSystem();

        authManager = createBasicSystemGraphRealm( config, logProvider, fileSystem );

        life.add( dependencies.dependencySatisfier().satisfyDependency( authManager ) );
        globalProcedures.registerProcedure( AuthProcedures.class );
    }

    @Override
    public AuthManager authManager()
    {
        return authManager;
    }

    public static final String USER_STORE_FILENAME = "auth";
    public static final String INITIAL_USER_STORE_FILENAME = "auth.ini";

    public static FileUserRepository getUserRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getUserRepositoryFile( config ), logProvider );
    }

    public static FileUserRepository getInitialUserRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getInitialUserRepositoryFile( config ), logProvider );
    }

    public static File getUserRepositoryFile( Config config )
    {
        return getUserRepositoryFile( config, USER_STORE_FILENAME );
    }

    public static File getInitialUserRepositoryFile( Config config )
    {
        return getUserRepositoryFile( config, INITIAL_USER_STORE_FILENAME );
    }

    private static File getUserRepositoryFile( Config config, String fileName )
    {
        // Resolve auth store file names

        // Because it contains sensitive information there is a legacy setting to configure
        // the location of the user store file that we still respect

        File authStore = config.get( GraphDatabaseSettings.auth_store ).toFile();
        if ( authStore.isFile() )
        {
            return authStore;
        }
        File authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory ).toFile();
        return new File( authStoreDir, fileName );
    }

    private BasicSystemGraphRealm createBasicSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        SecureHasher secureHasher = new SecureHasher();

        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> initialUserRepositorySupplier = () -> CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );

        UserSecurityGraphInitializer securityGraphInitializer =
                new UserSecurityGraphInitializer( databaseManager, systemGraphInitializer, logProvider.getLog( getClass() ),
                        migrationUserRepositorySupplier, initialUserRepositorySupplier, secureHasher );

        return new BasicSystemGraphRealm(
                securityGraphInitializer, // always init on start in community
                databaseManager,
                secureHasher,
                createAuthenticationStrategy( config ),
                true // native authentication in always enabled in community
        );
    }

    public static AuthenticationStrategy createAuthenticationStrategy( Config config )
    {
        return new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config );
    }
}
