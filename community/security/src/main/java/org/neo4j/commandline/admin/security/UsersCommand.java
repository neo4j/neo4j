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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.BasicAuthManagerFactory;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.time.Clocks;

public class UsersCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {

        public Provider()
        {
            super( "users" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "<subcommand> [<username>] [<password>] [--requires-password-change]" );
        }

        @Override
        public String description()
        {
            return "Runs several possible sub-commands for managing the native users repository: 'list', 'create', " +
                   "'delete' and 'set-password'. When creating a new user, it is created with a requirement to " +
                   "change password on first login. Use the option --requires-password-change=false to disable this. " +
                   "Passing a username to the 'list' command will do a case-insensitive substring search.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new UsersCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    public UsersCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Args parsedArgs = Args.parse( args );
        if ( parsedArgs.orphans().size() < 1 )
        {
            throw new IncorrectUsage(
                    "Missing arguments: expected at least one sub-command as argument: list, create, delete or " +
                    "set-password" );
        }

        String command = parsedArgs.orphans().size() > 0 ? parsedArgs.orphans().get( 0 ) : null;
        String username = parsedArgs.orphans().size() > 1 ? parsedArgs.orphans().get( 1 ) : null;
        String password = parsedArgs.orphans().size() > 2 ? parsedArgs.orphans().get( 2 ) : null;

        try
        {
            switch ( command.trim().toLowerCase() )
            {
            case "list":
                listUsers( username );
                break;
            case "set-password":
                if ( username == null || password == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'users set-password' expects username and password arguments" );
                }
                setPassword( username, password );
                break;
            case "create":
                if ( username == null || password == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'users create' expects username and password arguments" );
                }
                boolean requiresPasswordChange = !parsedArgs.asMap().containsKey( "requires-password-change" ) || (
                        parsedArgs.asMap().get( "requires-password-change" ).toLowerCase().equals( "true" ));
                createUser( username, password, requiresPasswordChange );
                break;
            case "delete":
                if ( username == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'users delete' expects username argument" );
                }
                deleteUser( username );
                break;
            default:
                throw new IncorrectUsage( "Unknown users command: " + command );
            }
        }
        catch ( IncorrectUsage e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failed run 'users " + command + "' on '" + username + "': " + e.getMessage(), e );
        }
        catch ( Throwable t )
        {
            throw new CommandFailed( "Failed run 'users " + command + "' on '" + username + "': " + t.getMessage(),
                    new RuntimeException( t.getMessage() ) );
        }
    }

    private void listUsers( String contains ) throws Throwable
    {
        getAuthManager();   // ensure defaults are created
        FileUserRepository userRepository = getUserRepository();
        for ( String username : userRepository.getAllUsernames() )
        {
            if ( contains == null || username.toLowerCase().contains( contains.toLowerCase() ) )
            {
                outsideWorld.stdOutLine( username );
            }
        }
    }

    private void createUser( String username, String password, boolean requiresPasswordChange ) throws Throwable
    {
        BasicAuthManager authManager = getAuthManager();
        authManager.newUser( username, password, requiresPasswordChange );
        outsideWorld.stdOutLine( "Created new user '" + username + "'" );
    }

    private void deleteUser( String username ) throws Throwable
    {
        BasicAuthManager authManager = getAuthManager();
        authManager.getUser( username );    // Will throw error on missing user
        if ( authManager.deleteUser( username ) )
        {
            outsideWorld.stdOutLine( "Deleted user '" + username + "'" );
        }
        else
        {
            outsideWorld.stdErrLine( "Failed to delete user '" + username + "'" );
        }
    }

    private void setPassword( String username, String password ) throws Throwable
    {
        BasicAuthManager authManager = getAuthManager();
        authManager.setUserPassword( username, password );
        outsideWorld.stdOutLine( "Changed password for user '" + username + "'" );
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

    private FileUserRepository getUserRepository() throws Throwable
    {
        Config config = loadNeo4jConfig( homeDir, configDir );
        FileUserRepository userRepository =
                BasicAuthManagerFactory.getUserRepository( config, NullLogProvider.getInstance() );
        userRepository.start();
        return userRepository;
    }

    private BasicAuthManager getAuthManager() throws Throwable
    {
        FileUserRepository userRepository = getUserRepository();
        PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        BasicAuthManager authManager = new BasicAuthManager( userRepository, passwordPolicy, Clocks.systemClock() );
        authManager.start();    // required to setup default users
        return authManager;
    }
}
