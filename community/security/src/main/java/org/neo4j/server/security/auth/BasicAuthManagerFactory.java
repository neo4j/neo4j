/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

/**
 * Wraps AuthManager and exposes it as a KernelExtension.
 */
@Service.Implementation( AuthManager.Factory.class )
public class BasicAuthManagerFactory extends AuthManager.Factory
{
    private static final String USER_STORE_FILENAME = "auth";

    public static FileUserRepository getUserRepository( Config config, LogProvider logProvider )
    {
        // Resolve auth store file names
        File authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory );

        // Because it contains sensitive information there is a legacy setting to configure
        // the location of the user store file that we still respect
        File userStoreFile = config.get( GraphDatabaseSettings.auth_store );
        if ( userStoreFile == null )
        {
            userStoreFile = new File( authStoreDir, USER_STORE_FILENAME );
        }
        return new FileUserRepository( userStoreFile.toPath(), logProvider );
    }

    public interface Dependencies
    {
        Config config();
        LogService logService();
    }

    public BasicAuthManagerFactory()
    {
        super( "basic-auth-manager" );
    }

    @Override
    public AuthManager newInstance( Config config, LogProvider logProvider )
    {
        if ( !config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            throw new IllegalStateException( "Attempted to build BasicAuthManager even though " +
                    "configuration setting auth_enabled=false" );
        }

        final UserRepository userRepository = getUserRepository( config, logProvider );

        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();

        return new BasicAuthManager( userRepository, passwordPolicy, Clocks.systemClock() );
    }
}
