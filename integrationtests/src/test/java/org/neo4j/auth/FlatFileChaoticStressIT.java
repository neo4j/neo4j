/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.auth;

import org.junit.Rule;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class FlatFileChaoticStressIT extends FlatFileStressBase
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
        ExecutorService service = Executors.newFixedThreadPool( n );
        Set<String> usernames = makeWithPrefix( "user", n );
        Set<String> roleNames = makeWithPrefix( "role", n );
        for ( int i = 0; i < n; i++ )
        {
            service.submit( new ChaoticAdmin( i, usernames, roleNames ) );
        }
        return service;
    }

    private Set<String> makeWithPrefix( String prefix, int n )
    {
        Set<String> set = new LinkedHashSet<>(  );
        IntStream.range( 0, n ).forEach( i -> set.add( prefix + i ) );
        return set;
    }

    private class ChaoticAdmin extends IrrationalAdmin
    {
        private final int number;
        private final String[] usernames;
        private final String[] roleNames;

        ChaoticAdmin( int number, Set<String> usernames, Set<String> roleNames )
        {
            super();
            this.number = number;
            this.usernames = usernames.toArray( new String[ usernames.size() ] );
            this.roleNames = roleNames.toArray( new String[ roleNames.size() ] );
            setActions(
                    this::createUser,
                    this::deleteUser,
                    this::changePassword,
                    this::suspend,
                    this::activate,
                    this::createRole,
                    this::deleteRole,
                    this::assignRole,
                    this::unAssignRole
                );
        }

        @Override
        public String toString()
        {
            return "ChaoticAdmin " + number;
        }

        // __________ ACTIONS ___________

        private void createUser()
        {
            String username = randomUser();
            String password = deviousPassword();
            try
            {
                flatFileRealm.newUser( username, password, false );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void deleteUser()
        {
            String username = randomUser();
            try
            {
                flatFileRealm.deleteUser( username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void changePassword()
        {
            String username = randomUser();
            String password = deviousPassword();
            try
            {
                flatFileRealm.setUserPassword( username, password, false );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void suspend()
        {
            String username = randomUser();
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
                // ignore
            }
        }

        private void activate()
        {
            String username = randomUser();
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
                // ignore
            }
        }

        private void createRole()
        {
            String username = randomUser();
            String roleName = randomRole();
            try
            {
                flatFileRealm.newRole( roleName, username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void deleteRole()
        {
            String roleName = randomRole();
            try
            {
                flatFileRealm.deleteRole( roleName );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void assignRole()
        {
            String username = randomUser();
            String roleName = randomRole();
            try
            {
                flatFileRealm.addRoleToUser( roleName, username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        private void unAssignRole()
        {
            String username = randomUser();
            String roleName = randomRole();
            try
            {
                flatFileRealm.removeRoleFromUser( roleName, username );
            }
            catch ( IOException e )
            {
                errors.add( e );
            }
            catch ( InvalidArgumentsException e )
            {
                // ignore
            }
        }

        // ______________ HELPERS ______________

        private String deviousPassword()
        {
            return random.nextBoolean() ? "123" : "321";
        }

        private String randomUser()
        {
            return usernames[ random.nextInt( usernames.length ) ];
        }

        private String randomRole()
        {
            return roleNames[ random.nextInt( roleNames.length ) ];
        }
    }
}
