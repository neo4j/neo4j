/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.auth;

import org.junit.Rule;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class FlatFilePredictableStressIT extends FlatFileStressBase
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Override
    FileSystemAbstraction getFileSystem()
    {
        return fsRule.get();
    }

    @Override
    ExecutorService setupWorkload( int n )
    {
        ExecutorService service = Executors.newFixedThreadPool( 2 * n );
        for ( int i = 0; i < n; i++ )
        {
            service.submit( new IrrationalUserAdmin( i ) );
            service.submit( new IrrationalRoleAdmin( i ) );
        }
        return service;
    }

    private class IrrationalUserAdmin extends IrrationalAdmin
    {
        private final String username;
        private String password;
        private boolean exists;

        IrrationalUserAdmin( int number )
        {
            super();
            username = "user" + number;
            password = deviousPassword();
            exists = false;
            setActions( this::createUser, this::deleteUser, this::changePassword, this::suspend,
                    this::activate, this::assignRole, this::unAssignRole );
        }

        @Override
        public String toString()
        {
            return "IrrationalUserAdmin " + username;
        }

        // __________ ACTIONS ___________

        private void createUser()
        {
            try
            {
                flatFileRealm.newUser( username, password, false );
                exists = true;
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !exists || !e.getMessage().contains( "The specified user '" + username + "' already exists" ) )
                {
                    errors.add( e );
                }
            }
        }

        private void deleteUser()
        {
            try
            {
                flatFileRealm.deleteUser( username );
                exists = false;
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void changePassword()
        {
            String newPassword = deviousPassword();
            try
            {
                flatFileRealm.setUserPassword( username, newPassword, false );
                password = newPassword;
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) && !validSamePassword( newPassword, e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void suspend()
        {
            try
            {
                flatFileRealm.suspendUser( username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void activate()
        {
            try
            {
                flatFileRealm.activateUser( username, false );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void assignRole()
        {
            String role = randomRole();
            try
            {
                flatFileRealm.addRoleToUser( role, username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void unAssignRole()
        {
            String role = randomRole();
            try
            {
                flatFileRealm.removeRoleFromUser( role, username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validUserDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        // ______________ HELPERS ______________

        private String deviousPassword()
        {
            return random.nextBoolean() ? "123" : "321";
        }

        private final String[] PREDEFINED_ROLES =
                {PredefinedRoles.READER, PredefinedRoles.PUBLISHER, PredefinedRoles.ARCHITECT, PredefinedRoles.ADMIN};

        private String randomRole()
        {
            return PREDEFINED_ROLES[ random.nextInt( PREDEFINED_ROLES.length ) ];
        }

        private boolean validSamePassword( String newPassword, InvalidArgumentsException e )
        {
            return newPassword.equals( password ) &&
                    e.getMessage().contains( "Old password and new password cannot be the same." );
        }

        private boolean validUserDoesNotExist( InvalidArgumentsException e )
        {
            return !exists && e.getMessage().contains( "User '" + username + "' does not exist" );
        }
    }

    private class IrrationalRoleAdmin extends IrrationalAdmin
    {
        private final String username;
        private final String roleName;
        private boolean exists;

        IrrationalRoleAdmin( int number )
        {
            username = "user" + number;
            roleName = "role" + number;
            exists = false;
            setActions( this::createRole, this::deleteRole, this::assignRole, this::unAssignRole );
        }

        @Override
        public String toString()
        {
            return "IrrationalRoleAdmin " + roleName;
        }

        // __________ ACTIONS ___________

        private void createRole()
        {
            try
            {
                flatFileRealm.newRole( roleName, username );
                exists = true;
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validRoleExists( e ) && !userDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void deleteRole()
        {
            try
            {
                flatFileRealm.deleteRole( roleName );
                exists = false;
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validRoleDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void assignRole()
        {
            try
            {
                flatFileRealm.addRoleToUser( roleName, "neo4j" );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validRoleDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        private void unAssignRole()
        {
            try
            {
                flatFileRealm.removeRoleFromUser( roleName, "neo4j" );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                if ( !validRoleDoesNotExist( e ) )
                {
                    errors.add( e );
                }
            }
        }

        // ______________ HELPERS ______________

        private boolean validRoleExists( InvalidArgumentsException e )
        {
            return exists && e.getMessage().contains( "The specified role '" + roleName + "' already exists" );
        }

        private boolean validRoleDoesNotExist( InvalidArgumentsException e )
        {
            return !exists && e.getMessage().contains( "Role '" + roleName + "' does not exist" );
        }

        private boolean userDoesNotExist( InvalidArgumentsException e )
        {
            return e.getMessage().contains( "User '" + username + "' does not exist" );
        }
    }
}
