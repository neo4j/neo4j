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
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;

public class SetDefaultAdminCommand implements AdminCommand
{
    public static final String ADMIN_INI = "admin.ini";
    public static final String COMMAND_NAME = "set-default-admin";
    public static final Arguments arguments = new Arguments()
            .withMandatoryPositionalArgument( 0, "username" );

    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( COMMAND_NAME );
        }

        @Override
        public Arguments allArguments()
        {
            return arguments;
        }

        @Override
        public String description()
        {
            return "Sets the user to become admin if users but no roles are present, " +
                    "for example when upgrading to neo4j 3.1 enterprise.";
        }

        @Override
        public String summary()
        {
            return "Sets the default admin user when no roles are present.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new SetDefaultAdminCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    SetDefaultAdminCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Args parsedArgs = validateArgs(args);

        try
        {
            setDefaultAdmin( parsedArgs.orphans().get( 0 ) );
        }
        catch ( IncorrectUsage e )
        {
            throw e;
        }
        catch ( Throwable throwable )
        {
            throw new CommandFailed( "Failed to execute 'set-default-admin' command: " + throwable.getMessage(),
                    new RuntimeException( throwable ) );
        }
    }

    private Args validateArgs( String[] args ) throws IncorrectUsage
    {
        Args parsedArgs = Args.parse( args );
        if ( parsedArgs.orphans().size() < 1 )
        {
            throw new IncorrectUsage( "No username specified." );
        }
        if ( parsedArgs.orphans().size() > 1 )
        {
            throw new IncorrectUsage( "Too many arguments." );
        }
        return parsedArgs;
    }

    private void setDefaultAdmin( String username ) throws Throwable
    {
        Config config = loadNeo4jConfig();
        File adminIniFile = new File( CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile(),
                ADMIN_INI );
        FileSystemAbstraction fileSystem = outsideWorld.fileSystem();
        if ( fileSystem.fileExists( adminIniFile ) )
        {
            fileSystem.deleteFile( adminIniFile );
        }
        UserRepository users = new FileUserRepository( fileSystem, adminIniFile, NullLogProvider.getInstance() );
        users.init();
        users.start();
        users.create( new User.Builder( username, Credential.INACCESSIBLE ).build() );
        users.stop();
        users.shutdown();

        outsideWorld.stdOutLine( "Set default admin to '" + username + "'." );
    }

    Config loadNeo4jConfig()
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        return configLoader.loadOfflineConfig(
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
