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

import static org.neo4j.server.security.auth.UserManager.INITIAL_USER_NAME;

public class SetInitialPasswordCommand implements AdminCommand
{

    public static final Arguments arguments = new Arguments()
            .withMandatoryPositionalArgument( 0, "password" );

    public static class Provider extends AdminCommand.Provider
    {

        public Provider()
        {
            super( "set-initial-password" );
        }

        @Override
        public Arguments allArguments()
        {
            return arguments;
        }

        @Override
        public String description()
        {
            return "Sets the initial password of the initial admin user ('" + INITIAL_USER_NAME + "').";
        }

        @Override
        public String summary()
        {
            return description();
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new SetInitialPasswordCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    SetInitialPasswordCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
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
            setPassword( parsedArgs.orphans().get( 0 ) );
        }
        catch ( IncorrectUsage e )
        {
            throw e;
        }
        catch ( Throwable throwable )
        {
            throw new CommandFailed( "Failed to execute 'set-initial-password' command: " + throwable.getMessage(),
                    new RuntimeException( throwable ) );
        }
    }

    private Args validateArgs( String[] args ) throws IncorrectUsage
    {
        Args parsedArgs = Args.parse( args );
        if ( parsedArgs.orphans().size() < 1 )
        {
            throw new IncorrectUsage( "No password specified." );
        }
        if ( parsedArgs.orphans().size() > 1 )
        {
            throw new IncorrectUsage( "Too many arguments." );
        }
        return parsedArgs;
    }

    private void setPassword( String password ) throws Throwable
    {
        Config config = loadNeo4jConfig();
        if ( realUsersExist( config ) )
        {
            outsideWorld.stdOutLine( "Warning: Initial password was not set because live Neo4j-users were " +
                    "detected, so the initial password has no effect." );
        }
        else
        {
            File file = CommunitySecurityModule.getInitialUserRepositoryFile( config );
            FileSystemAbstraction fileSystem = outsideWorld.fileSystem();
            if ( fileSystem.fileExists( file ) )
            {
                fileSystem.deleteFile( file );
            }

            FileUserRepository userRepository =
                    new FileUserRepository( fileSystem, file, NullLogProvider.getInstance() );
            userRepository.start();
            userRepository.create(
                    new User.Builder( INITIAL_USER_NAME, Credential.forPassword( password ) )
                            .withRequiredPasswordChange( false )
                            .build()
                );
            userRepository.shutdown();
            outsideWorld.stdOutLine( "Changed password for user '" + INITIAL_USER_NAME + "'." );
        }
    }

    private boolean realUsersExist( Config config )
    {
        File authFile = CommunitySecurityModule.getUserRepositoryFile( config );
        return outsideWorld.fileSystem().fileExists( authFile );
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
