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
package org.neo4j.commandline.admin.security;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;

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
import org.neo4j.server.security.auth.UserRepository;

public class SetDefaultAdminCommand implements AdminCommand
{
    public static final String ADMIN_INI = "admin.ini";
    public static final String COMMAND_NAME = "set-default-admin";
    private static final Arguments arguments = new Arguments().withMandatoryPositionalArgument( 0, "username" );

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    SetDefaultAdminCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
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
            setDefaultAdmin( arguments.parse( args ).get( 0 ) );
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

    private void setDefaultAdmin( String username ) throws Throwable
    {
        FileSystemAbstraction fileSystem = outsideWorld.fileSystem();
        Config config = loadNeo4jConfig();

        FileUserRepository users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(),
                fileSystem );

        users.init();
        users.start();
        Set<String> userNames = users.getAllUsernames();
        users.stop();
        users.shutdown();

        if ( userNames.isEmpty() )
        {
            FileUserRepository initialUsers = CommunitySecurityModule.getInitialUserRepository( config,
                    NullLogProvider.getInstance(),
                    fileSystem );
            initialUsers.init();
            initialUsers.start();
            userNames = initialUsers.getAllUsernames();
            initialUsers.stop();
            initialUsers.shutdown();
        }

        if ( !userNames.contains( username ) )
        {
            throw new CommandFailed( String.format( "no such user: '%s'", username ) );
        }

        File adminIniFile = new File( CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile(),
                ADMIN_INI );
        if ( fileSystem.fileExists( adminIniFile ) )
        {
            fileSystem.deleteFile( adminIniFile );
        }
        UserRepository admins = new FileUserRepository( fileSystem, adminIniFile, NullLogProvider.getInstance() );
        admins.init();
        admins.start();
        admins.create( new User.Builder( username, Credential.INACCESSIBLE ).build() );
        admins.stop();
        admins.shutdown();

        outsideWorld.stdOutLine( "default admin user set to '" + username + "'" );
    }

    Config loadNeo4jConfig()
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled().build();
    }
}
