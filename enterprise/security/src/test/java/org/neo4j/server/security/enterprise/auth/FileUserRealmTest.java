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
package org.neo4j.server.security.enterprise.auth;

import com.google.testing.threadtester.ClassInstrumentation;
import com.google.testing.threadtester.CodePosition;
import com.google.testing.threadtester.Instrumentation;
import com.google.testing.threadtester.InterleavedRunner;
import com.google.testing.threadtester.MainRunnableImpl;
import com.google.testing.threadtester.RunResult;
import com.google.testing.threadtester.SecondaryRunnableImpl;
import com.google.testing.threadtester.ThreadedTest;
import com.google.testing.threadtester.ThreadedTestRunner;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileUserRealmTest
{
    RoleRepository roleRepository;
    UserRepository userRepository;

    private static final String USERNAME = "neo4j";
    private static final String ROLE = "admin";

    public FileUserRealmTest() throws Exception
    {
        super();
        setup();
    }

    private void setup() throws Exception
    {
        roleRepository = new InMemoryRoleRepository();
        userRepository = new InMemoryUserRepository();
        userRepository.create( new User.Builder().withName( USERNAME ).build() );
        roleRepository.create( new RoleRecord.Builder().withName( ROLE ).build() );
    }

    @Test
    public void testThreadedTests() throws Exception
    {
        ThreadedTestRunner runner = new ThreadedTestRunner();

        try
        {
            runner.runTests( getClass(), FileUserRealm.class );
        }
        // We need to work around an issue that we do not get failures from the test framework
        catch ( final RuntimeException e )
        {
            final Throwable root = org.apache.commons.lang3.exception.ExceptionUtils.getRootCause( e );
            if ( root instanceof AssertionError )
            {
                throw (AssertionError) root;
            }
            else
            {
                throw e;
            }
        }
    }

    @ThreadedTest
    public void addUserToRoleShouldBeAtomic() throws Exception
    {
        // Create a code position for where we want to break in the main thread
        CodePosition codePosition = getCodePositionAfterCall( "addUserToRole", "findByName" );

        FileUserRealm realm = new FileUserRealm( userRepository, roleRepository );

        // When
        RunResult result = InterleavedRunner.interleave(
                new AddUserToRoleInMain( realm ),
                new DeleteUserInSecondary(),
                Arrays.asList( codePosition ) );
        result.throwExceptionsIfAny();

        // Then
        RoleRecord role = roleRepository.findByName( ROLE );
        assertNull( "User " + USERNAME + " should be deleted!", userRepository.findByName( USERNAME ) );
        assertNotNull( "Role " + ROLE + " should exist!", role );
        assertTrue( "Users assigned to role " + ROLE + " should be empty!", role.users().isEmpty() );
    }

    @ThreadedTest
    public void deleteUserShouldBeAtomic() throws Exception
    {
        // Create a code position for where we want to break in the main thread
        CodePosition codePosition = getCodePositionAfterCall( "deleteUser", "findByName" );

        FileUserRealm realm = new FileUserRealm( userRepository, roleRepository );

        // When
        RunResult result = InterleavedRunner.interleave(
                new DeleteUserInMain( realm ),
                new AddUserToRoleInSecondary(),
                Arrays.asList( codePosition ) );

        // Then
        assertNull( "User " + USERNAME + " should be deleted!", userRepository.findByName( USERNAME ) );
        assertTrue( result.getSecondaryException() instanceof IllegalArgumentException );
        assertTrue( result.getSecondaryException().getMessage().equals( "User " + USERNAME + " does not exist." ) );
    }

    private CodePosition getCodePositionAfterCall( String caller, String called )
    {
        ClassInstrumentation instrumentation = Instrumentation.getClassInstrumentation( FileUserRealm.class );
        CodePosition codePosition = instrumentation.afterCall( caller, called );
        return codePosition;
    }

    // Base class for the main thread
    private class AdminMain extends MainRunnableImpl<FileUserRealm>
    {
        protected FileUserRealm realm;

        public AdminMain( FileUserRealm realm )
        {
            this.realm = realm;
        }

        @Override
        public Class<FileUserRealm> getClassUnderTest()
        {
            return FileUserRealm.class;
        }

        @Override
        public FileUserRealm getMainObject()
        {
            return realm;
        }

        @Override
        public void run() throws Exception
        {
        }
    }

    // Base class for the secondary thread
    private class AdminSecondary extends SecondaryRunnableImpl<FileUserRealm,AdminMain>
    {
        protected FileUserRealm realm;

        @Override
        public void initialize( AdminMain main ) throws Exception
        {
            realm = main.getMainObject();
        }

        @Override
        public void run() throws Exception
        {
        }
    }

    // NOTE: We avoid parameterizing the operation using lambdas because it seemed to cause stability problems
    // with the instrumentation/reflection.

    //-----------------
    // Add user to role
    private class AddUserToRoleInMain extends AdminMain
    {
        public AddUserToRoleInMain( FileUserRealm realm )
        {
            super( realm );
        }

        @Override
        public void run() throws Exception
        {
            realm.addUserToRole( USERNAME, ROLE );
        }
    }

    private class AddUserToRoleInSecondary extends AdminSecondary
    {
        @Override
        public void run() throws Exception
        {
            realm.addUserToRole( USERNAME, ROLE );
        }
    }

    //-----------------
    // Delete user
    private class DeleteUserInMain extends AdminMain
    {
        public DeleteUserInMain( FileUserRealm realm )
        {
            super( realm );
        }

        @Override
        public void run() throws Exception
        {
            realm.deleteUser( ROLE );
        }
    }

    private class DeleteUserInSecondary extends AdminSecondary
    {
        @Override
        public void run() throws Exception
        {
            realm.deleteUser( USERNAME );
        }
    }
}
