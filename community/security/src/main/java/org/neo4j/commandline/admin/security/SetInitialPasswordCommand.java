/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

public class SetInitialPasswordCommand implements AdminCommand
{

    private static final Arguments arguments = new Arguments().withMandatoryPositionalArgument( 0, "password" );

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    SetInitialPasswordCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    public static Arguments arguments()
    {
        return arguments;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            setPassword( arguments.parse( args ).get( 0 ) );
        }
        catch ( IncorrectUsage | CommandFailed e )
        {
            throw e;
        }
        catch ( Throwable throwable )
        {
            throw new CommandFailed( throwable.getMessage(), new RuntimeException( throwable ) );
        }
    }

    private void setPassword( String password ) throws Throwable
    {
        Config config = loadNeo4jConfig();
        if ( realUsersExist( config ) )
        {
            throw new CommandFailed( "initial password was not set because live Neo4j-users were detected." );
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
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile() )
                .withHome( homeDir.toFile() )
                .withConnectorsDisabled().build();
    }
}
