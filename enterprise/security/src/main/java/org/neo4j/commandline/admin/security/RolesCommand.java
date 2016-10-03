/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthManagerFactory;
import org.neo4j.server.security.enterprise.auth.RoleRepository;

public class RolesCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {

        public Provider()
        {
            super( "roles" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "<subcommand> [<roleName>] [<username>]" );
        }

        @Override
        public String description()
        {
            return "Runs several possible sub-commands for managing the native roles repository: " +
                   "'list', 'create', 'delete', 'assign', 'remove', 'for' and 'users'. " +
                   "'list' with no arguments lists all roles, and with an argument does a substring " +
                   "filter. 'create' and 'delete' simply create and delete the specified role. " +
                   "'assign' and 'remove' take two arguments, the role name and the username and " +
                   "assign the role to the user, or unassign the role from the user. The 'for' command " +
                   "takes one argument, the username, and will list all roles for that user. The 'users' " +
                   "command takes one argument, the role name, and will list all users for that role.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new RolesCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;
    private EnterpriseAuthAndUserManager authManager;

    private RolesCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
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
                    "Missing arguments: expected at least one sub-command as argument: " +
                    "'list', 'create', 'delete', 'assign', 'remove', 'for' or 'users'." );
        }

        String command = parsedArgs.orphans().size() > 0 ? parsedArgs.orphans().get( 0 ) : null;
        String roleName = parsedArgs.orphans().size() > 1 ? parsedArgs.orphans().get( 1 ) : null;
        String username = parsedArgs.orphans().size() > 2 ? parsedArgs.orphans().get( 2 ) : null;

        try
        {
            switch ( command.trim().toLowerCase() )
            {
            case "list":
                listRoles( roleName );
                break;
            case "create":
                if ( roleName == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'roles create' expects roleName argument" );
                }
                createRole( roleName );
                break;
            case "delete":
                if ( roleName == null )
                {
                    throw new IncorrectUsage( "Missing arguments: 'roles delete' expects roleName argument" );
                }
                deleteRole( roleName );
                break;
            case "assign":
                if ( roleName == null || username == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'roles assign' expects roleName and username arguments" );
                }
                assignRole( roleName, username );
                break;
            case "remove":
                if ( roleName == null || username == null )
                {
                    throw new IncorrectUsage(
                            "Missing arguments: 'roles remove' expects roleName and username arguments" );
                }
                removeRole( roleName, username );
                break;
            case "for":
                if ( roleName == null )
                {
                    throw new IncorrectUsage( "Missing arguments: 'roles for' expects username argument" );
                }
                rolesFor( roleName );
                break;
            case "users":
                if ( roleName == null )
                {
                    throw new IncorrectUsage( "Missing arguments: 'roles users' expects roleName argument" );
                }
                usersFor( roleName );
                break;
            default:
                throw new IncorrectUsage( "Unknown roles command: " + command );
            }
        }
        catch ( IncorrectUsage e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failed to run 'roles " + command + "' on '" + roleName + "': " + e.getMessage(),
                    e );
        }
        catch ( Throwable t )
        {
            throw new CommandFailed( "Failed to run 'roles " + command + "' on '" + roleName + "': " + t.getMessage(),
                    new RuntimeException( t.getMessage() ) );
        }
    }

    private void listRoles( String roleName ) throws Throwable
    {
        getAuthManager();   // ensure defaults are created
        RoleRepository roleRepository = getRoleRepository();
        roleRepository.getAllRoleNames().stream()
                .filter( r -> roleName == null || r.toLowerCase().contains( roleName ) )
                .forEach( outsideWorld::stdOutLine );
    }

    private void createRole( String roleName ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().newRole( roleName );
        outsideWorld.stdOutLine( "Created new role '" + roleName + "'" );
    }

    private void deleteRole( String roleName ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().getRole( roleName ); // Will throw error on missing role
        if ( authManager.getUserManager().deleteRole( roleName ) )
        {
            outsideWorld.stdOutLine( "Deleted role '" + roleName + "'" );
        }
        else
        {
            outsideWorld.stdErrLine( "Failed to delete role '" + roleName + "'" );
        }
    }

    private void assignRole( String roleName, String username ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().getRole( roleName ); // Will throw error on missing role
        authManager.getUserManager().getUser( username ); // Will throw error on missing user
        for ( String name : authManager.getUserManager().getUsernamesForRole( roleName ) )
        {
            if ( name.equals( username ) )
            {
                throw new InvalidArgumentsException( "Role '" + roleName + "' was already assigned to user 'another'" );
            }
        }
        authManager.getUserManager().addRoleToUser( roleName, username );
        outsideWorld.stdOutLine( "Assigned role '" + roleName + "' to user '" + username + "'" );
    }

    private void removeRole( String roleName, String username ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().getRole( roleName ); // Will throw error on missing role
        authManager.getUserManager().getUser( username ); // Will throw error on missing user
        for ( String name : authManager.getUserManager().getUsernamesForRole( roleName ) )
        {
            if ( name.equals( username ) )
            {
                authManager.getUserManager().removeRoleFromUser( roleName, username );
                outsideWorld.stdOutLine( "Removed role '" + roleName + "' from user '" + username + "'" );
                return;
            }
        }
        throw new InvalidArgumentsException( "Role '" + roleName + "' was not assigned to user 'another'" );
    }

    private void rolesFor( String username ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().getUser( username ); // Will throw error on missing user
        for ( String roleName : authManager.getUserManager().getRoleNamesForUser( username ) )
        {
            outsideWorld.stdOutLine( roleName );
        }
    }

    private void usersFor( String roleName ) throws Throwable
    {
        EnterpriseAuthAndUserManager authManager = getAuthManager();
        authManager.getUserManager().getRole( roleName ); // Will throw error on missing role
        for ( String username : authManager.getUserManager().getUsernamesForRole( roleName ) )
        {
            outsideWorld.stdOutLine( username );
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

    private RoleRepository getRoleRepository() throws Throwable
    {
        Config config = loadNeo4jConfig( homeDir, configDir );
        RoleRepository repo = EnterpriseAuthManagerFactory
                .getRoleRepository( config, NullLogProvider.getInstance(), outsideWorld.fileSystem() );
        repo.start();
        return repo;
    }

    private EnterpriseAuthAndUserManager getAuthManager() throws Throwable
    {
        if ( this.authManager == null )
        {
            Config config = loadNeo4jConfig( homeDir, configDir );
            JobScheduler jobScheduler = new UsersCommand.NoOpJobScheduler();
            this.authManager = new EnterpriseAuthManagerFactory()
                    .newInstance( config, NullLogProvider.getInstance(),
                            NullLog.getInstance(), outsideWorld.fileSystem(), jobScheduler );
            this.authManager.start();    // required to setup default roles
        }
        return this.authManager;
    }
}
