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
package org.neo4j.commandline.admin.security;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;

import static java.time.Clock.systemUTC;
import static org.neo4j.dbms.DatabaseManagementSystemSettings.auth_store_directory;

public class SetPasswordCommand implements AdminCommand
{
    public class Provider extends AdminCommand.Provider
    {

        protected Provider()
        {
            super( "set-password" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "<username> <password>" );
        }

        @Override
        public String description()
        {
            return "Sets the password for the specified user and removes the password change requirement";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir )
        {
            return new SetPasswordCommand( homeDir, configDir );
        }
    }

    private final Path homeDir;
    private final Path configDir;

    public SetPasswordCommand( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        if ( args.length < 2 )
        {
            throw new IncorrectUsage( "Missing arguments: expected username and password" );
        }
        String username = args[0];
        String password = args[1];
        try
        {
            Config config = loadNeo4jConfig( homeDir, configDir );
            File authDir = config.get( auth_store_directory );
            FileUserRepository userRepository =
                    new FileUserRepository( new File( authDir, "auth.db" ).toPath(), NullLogProvider.getInstance() );
            userRepository.start();
            PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
            BasicAuthManager authManager = new BasicAuthManager( userRepository, passwordPolicy, systemUTC(), true );
            authManager.setUserPassword( username, password );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failed to set password for '" + username + "': " + e.getMessage(), e );
        }
        catch ( Throwable t )
        {
            throw new CommandFailed( "Failed to set password for '" + username + "': " + t.getMessage(), new RuntimeException(t.getMessage()) );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        return configLoader.loadConfig(
                Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) );
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }
}
