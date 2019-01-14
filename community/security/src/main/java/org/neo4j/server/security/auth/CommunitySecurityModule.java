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

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

@Service.Implementation( SecurityModule.class )
public class CommunitySecurityModule extends SecurityModule
{
    public CommunitySecurityModule()
    {
        super( CommunityEditionModule.COMMUNITY_SECURITY_MODULE_ID );
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        Config config = dependencies.config();
        Procedures procedures = dependencies.procedures();
        LogProvider logProvider = dependencies.logService().getUserLogProvider();
        FileSystemAbstraction fileSystem = dependencies.fileSystem();
        final UserRepository userRepository = getUserRepository( config, logProvider, fileSystem );
        final UserRepository initialUserRepository = getInitialUserRepository( config, logProvider, fileSystem );

        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();

        BasicAuthManager authManager = new BasicAuthManager( userRepository, passwordPolicy, Clocks.systemClock(),
                initialUserRepository, config );

        dependencies.lifeSupport().add( dependencies.dependencySatisfier().satisfyDependency( authManager ) );

        procedures.registerComponent( UserManager.class, ctx -> authManager, false );
        procedures.registerProcedure( AuthProcedures.class );
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
        File authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory );

        // Because it contains sensitive information there is a legacy setting to configure
        // the location of the user store file that we still respect
        File userStoreFile = config.get( GraphDatabaseSettings.auth_store );
        if ( userStoreFile == null )
        {
            userStoreFile = new File( authStoreDir, fileName );
        }
        return userStoreFile;
    }
}
