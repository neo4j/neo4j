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
package org.neo4j.auth;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import org.neo4j.server.security.enterprise.auth.InternalFlatFileRealm;
import org.neo4j.server.security.enterprise.auth.RoleRecord;
import org.neo4j.server.security.enterprise.auth.RoleRepository;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertTrue;

public class FlatFilePredictableStressIT
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private InternalFlatFileRealm flatFileRealm;
    private UserRepository userRepository;
    private RoleRepository roleRepository;

    private volatile boolean keepRunning = true;
    private final Set<Throwable> errors = ConcurrentHashMap.newKeySet();

    @Before
    public void setup() throws Throwable
    {
        Config config = Config.defaults();
        LogProvider logProvider = NullLogProvider.getInstance();
        JobScheduler jobScheduler = new NoopJobScheduler();

        userRepository = CommunitySecurityModule.getUserRepository( config, logProvider, fsRule.get() );
        roleRepository = EnterpriseSecurityModule.getRoleRepository( config, logProvider, fsRule.get() );

        flatFileRealm = new InternalFlatFileRealm(
                userRepository,
                roleRepository,
                new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ),
                jobScheduler,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, fsRule.get() )
            );

        flatFileRealm.init();
        flatFileRealm.start();
    }

    @After
    public void teardown() throws Throwable
    {
        flatFileRealm.stop();
        flatFileRealm.shutdown();
    }

    @Test
    public void shouldMaintainConsistency() throws InterruptedException, IOException
    {
        final long ONE_SECOND = 1000;
        final long TIMEOUT_IN_SECONDS = 10;
        final int N = 10;
        final int ERROR_LIMIT = 100;

        ExecutorService service = Executors.newFixedThreadPool( 2 * N );
        for ( int i = 0; i < N; i++ )
        {
            service.submit( new IrrationalUserAdmin( i ) );
            service.submit( new IrrationalRoleAdmin( i ) );
        }

        for ( int t = 0; t < TIMEOUT_IN_SECONDS; t++ )
        {
            Thread.sleep( ONE_SECOND );
            if ( errors.size() > ERROR_LIMIT )
            {
                break;
            }
        }

        keepRunning = false;
        service.shutdown();
        service.awaitTermination( 5, SECONDS );

        // Assert that no errors occured
        String msg = String.join( System.lineSeparator(),
                errors.stream().map( Throwable::getMessage ).collect( Collectors.toList() ) );
        assertThat( msg, errors, empty() );

        // Assert that user and role repos are consistent
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();
        assertTrue(
                "User and role repositories are no longer consistent",
                RoleRepository.validate( users.values(), roles.values() )
            );
    }

    abstract class IrrationalAdmin implements Runnable
    {
        final Random random = new Random();
        Runnable[] actions;

        @Override
        public void run()
        {
            while ( keepRunning )
            {
                randomAction().run();
            }
        }

        private Runnable randomAction()
        {
            return actions[ random.nextInt( actions.length ) ];
        }

        void setActions( Runnable... actions )
        {
            this.actions = actions;
        }
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

    private class NoopJobScheduler extends LifecycleAdapter implements JobScheduler
    {
        @Override
        public Executor executor( Group group )
        {
            return null;
        }

        @Override
        public ThreadFactory threadFactory( Group group )
        {
            return null;
        }

        @Override
        public JobHandle schedule( Group group, Runnable job )
        {
            return null;
        }

        @Override
        public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
        {
            return null;
        }

        @Override
        public JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit )
        {
            return null;
        }

        @Override
        public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
        {
            return null;
        }

        @Override
        public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period,
        TimeUnit timeUnit )
        {
            return null;
        }
    }
}
