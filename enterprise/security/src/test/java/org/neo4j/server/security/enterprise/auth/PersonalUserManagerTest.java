/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.time.Clock;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PersonalUserManagerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private PersonalUserManager userManager;
    private EvilUserManager evilUserManager;
    private Log log;

    @Test
    public void shouldHandleFailureToCreateUser() throws Exception
    {
        // Given
        evilUserManager.setFailNextCall();

        //Expect
        expectedException.expect( IOException.class );
        expectedException.expectMessage( "newUserException" );

        // When
        userManager.newUser( "hewhoshallnotbenamed", "avada kedavra", false );
        verify( log ).error( withSubject( SecurityContext.AUTH_DISABLED.subject(), "tried to create user `%s`: %s" ),
                "hewhoshallnotbenamed", "newUserException" );
    }

    @Before
    public void setup()
    {
        evilUserManager = new EvilUserManager(
                new InternalFlatFileRealm( new InMemoryUserRepository(), new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(), new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ),
                        new InternalFlatFileRealmIT.TestJobScheduler(), new InMemoryUserRepository(),
                        new InMemoryUserRepository() ) );
        log = spy( Log.class );
        userManager = new PersonalUserManager( evilUserManager, AuthSubject.AUTH_DISABLED, new SecurityLog( log ), true );
    }

    private String withSubject( AuthSubject subject, String msg )
    {
        return "[" + subject.username() + "] " + msg;
    }

    private class EvilUserManager implements EnterpriseUserManager
    {
        private boolean failNextCall;
        private EnterpriseUserManager delegate;

        EvilUserManager( EnterpriseUserManager delegate )
        {
            this.delegate = delegate;
        }

        void setFailNextCall()
        {
            failNextCall = true;
        }

        @Override
        public User newUser( String username, String password, boolean changeRequired )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "newUserException" );
            }
            return delegate.newUser( username, password, changeRequired );
        }

        @Override
        public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "deleteUserException" );
            }
            return delegate.deleteUser( username );
        }

        @Override
        public User getUser( String username ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getUserException" );
            }
            return delegate.getUser( username );
        }

        @Override
        public User silentlyGetUser( String username )
        {
            return delegate.silentlyGetUser( username );
        }

        @Override
        public void setUserPassword( String username, String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "setUserPasswordException" );
            }
            delegate.setUserPassword( username, password, requirePasswordChange );
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return delegate.getAllUsernames();
        }

        @Override
        public void suspendUser( String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "suspendUserException" );
            }
            delegate.suspendUser( username );
        }

        @Override
        public void activateUser( String username, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "activateUserException" );
            }
            delegate.activateUser( username, requirePasswordChange );
        }

        @Override
        public RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "newRoleException" );
            }
            return delegate.newRole( roleName, usernames );
        }

        @Override
        public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "deleteRoleException" );
            }
            return delegate.deleteRole( roleName );
        }

        @Override
        public RoleRecord getRole( String roleName ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getRoleException" );
            }
            return delegate.getRole( roleName );
        }

        @Override
        public RoleRecord silentlyGetRole( String roleName )
        {
            return delegate.silentlyGetRole( roleName );
        }

        @Override
        public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "addRoleToUserException" );
            }
            delegate.addRoleToUser( roleName, username );
        }

        @Override
        public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "removeRoleFromUserException" );
            }
            delegate.removeRoleFromUser( roleName, username );
        }

        @Override
        public Set<String> getAllRoleNames()
        {
            return delegate.getAllRoleNames();
        }

        @Override
        public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getRoleNamesForUserException" );
            }
            return delegate.getRoleNamesForUser( username );
        }

        @Override
        public Set<String> silentlyGetRoleNamesForUser( String username )
        {
            return delegate.silentlyGetRoleNamesForUser( username );
        }

        @Override
        public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getUsernamesForRoleException" );
            }
            return delegate.getUsernamesForRole( roleName );
        }

        @Override
        public Set<String> silentlyGetUsernamesForRole( String roleName )
        {
            return delegate.silentlyGetUsernamesForRole( roleName );
        }
    }
}
