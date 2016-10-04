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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.security.auth.UserManager;
import org.neo4j.server.security.auth.UserManagerSupplier;

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

    UsersCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
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
        boolean requiresPasswordChange = !hasFlagWithValue( parsedArgs, "requires-password-change", "false" );

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
                setPassword( username, password, requiresPasswordChange );
                break;
            case "create":
                if ( username == null || password == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'users create' expects username and password arguments" );
                }
                createUser( username, password, requiresPasswordChange );
                break;
            case "delete":
                if ( username == null )
                {
                    throw new IncorrectUsage( "Missing arguments: 'users delete' expects username argument" );
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

    private boolean hasFlagWithValue( Args parsedArgs, String key, String expectedValue )
    {
        Map<String,String> argsMap = parsedArgs.asMap();
        return argsMap.containsKey( key ) && (argsMap.get( key ).trim().toLowerCase().equals( expectedValue ));
    }

    private void listUsers( String contains ) throws Throwable
    {
        UserManager userManager = getUserManager();
        for ( String username : userManager.getAllUsernames() )
        {
            if ( contains == null || username.toLowerCase().contains( contains.toLowerCase() ) )
            {
                outsideWorld.stdOutLine( username );
            }
        }
    }

    private void createUser( String username, String password, boolean requiresPasswordChange ) throws Throwable
    {
        UserManager userManager = getUserManager();
        userManager.newUser( username, password, requiresPasswordChange );
        outsideWorld.stdOutLine( "Created new user '" + username + "'" );
    }

    private void deleteUser( String username ) throws Throwable
    {
        UserManager userManager = getUserManager();
        userManager.getUser( username );    // Will throw error on missing user
        if ( userManager.getAllUsernames().size() == 1 )
        {
            throw new IllegalArgumentException( "Deleting the only remaining user '" + username + "' is not allowed" );
        }
        if ( userManager.deleteUser( username ) )
        {
            outsideWorld.stdOutLine( "Deleted user '" + username + "'" );
        }
        else
        {
            outsideWorld.stdErrLine( "Failed to delete user '" + username + "'" );
        }
    }

    private void setPassword( String username, String password, boolean requirePasswordChange ) throws Throwable
    {
        UserManager userManager = getUserManager();
        userManager.setUserPassword( username, password, requirePasswordChange );
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

    private UserManager getUserManager() throws Throwable
    {
        Config config = loadNeo4jConfig( homeDir, configDir );
        String configuredKey = config.get( GraphDatabaseSettings.auth_manager );
        List<AuthManager.Factory> wantedAuthManagerFactories = new ArrayList<>();
        List<AuthManager.Factory> backupAuthManagerFactories = new ArrayList<>();

        for ( AuthManager.Factory candidate : Service.load( AuthManager.Factory.class ) )
        {
            if ( StreamSupport.stream( candidate.getKeys().spliterator(), false ).anyMatch( configuredKey::equals ) )
            {
                wantedAuthManagerFactories.add( candidate );
            }
            else
            {
                backupAuthManagerFactories.add( candidate );
            }
        }

        AuthManager authManager = tryMakeInOrder( config, wantedAuthManagerFactories );

        if ( authManager == null )
        {
            authManager = tryMakeInOrder( config, backupAuthManagerFactories );
        }

        if ( authManager == null )
        {
            outsideWorld.stdErrLine( "No auth manager implementation specified and no default could be loaded. " +
                    "It is an illegal product configuration to have auth enabled and not provide an auth manager service." );
            throw new IllegalArgumentException(
                    "Auth enabled but no auth manager found. This is an illegal product configuration." );
        }
        if ( authManager instanceof UserManagerSupplier )
        {
            authManager.start();
            return ((UserManagerSupplier) authManager).getUserManager();
        }
        else
        {
            outsideWorld.stdErrLine( "The configured auth manager doesn't handle user management." );
            throw new IllegalArgumentException( "The configured auth manager doesn't handle user management." );
        }
    }

    private AuthManager tryMakeInOrder( Config config, List<AuthManager.Factory> authManagerFactories )
    {
        JobScheduler jobScheduler = JobScheduler.NO_OP;
        for ( AuthManager.Factory x : authManagerFactories )
        {
            try
            {
                return x.newInstance( config, NullLogProvider.getInstance(), NullLog.getInstance(),
                        outsideWorld.fileSystem(), jobScheduler );
            }
            catch ( Exception e )
            {
                outsideWorld.stdOutLine(
                        String.format( "Attempted to load configured auth manager with keys '%s', but failed",
                                String.join( ", ", x.getKeys() ) ) );
            }
        }
        return null;
    }
}
