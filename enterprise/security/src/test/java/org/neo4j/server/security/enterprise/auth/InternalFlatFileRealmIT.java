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
package org.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.time.Clocks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.fail;

public class InternalFlatFileRealmIT
{
    File userStoreFile;
    File roleStoreFile;

    TestJobScheduler jobScheduler = new TestJobScheduler();
    LogProvider logProvider = NullLogProvider.getInstance();
    InternalFlatFileRealm realm;
    EvilFileSystem fs;

    private static int LARGE_NUMBER = 123;

    @BeforeEach
    public void setup() throws Throwable
    {
        fs = new EvilFileSystem( new EphemeralFileSystemAbstraction() );
        userStoreFile = new File( "dbms", "auth" );
        roleStoreFile = new File( "dbms", "roles" );
        final UserRepository userRepository = new FileUserRepository( fs, userStoreFile, logProvider );
        final RoleRepository roleRepository = new FileRoleRepository( fs, roleStoreFile, logProvider );
        final UserRepository initialUserRepository = CommunitySecurityModule.getInitialUserRepository( Config
                .defaults(), logProvider, fs );
        final UserRepository defaultAdminRepository = EnterpriseSecurityModule.getDefaultAdminRepository( Config
                .defaults(), logProvider, fs );
        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        AuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 );

        realm = new InternalFlatFileRealm( userRepository, roleRepository, passwordPolicy, authenticationStrategy,
                        true, true, jobScheduler, initialUserRepository, defaultAdminRepository );
        realm.init();
        realm.start();
    }

    @AfterEach
    public void tearDown() throws Throwable
    {
        realm.shutdown();
        fs.close();
    }

    @Test
    public void shouldReloadAuthFiles() throws Exception
    {
        fs.addUserRoleFilePair(
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                "Carol:SHA-256,FE0056C37E,A543:\n" +
                "Mia:SHA-256,0E1FFFC23E,34A4:password_change_required\n"
                ,
                "admin:Mia\n" +
                "publisher:Hanna,Carol\n" );

        jobScheduler.scheduledRunnable.run();

        assertThat( realm.getAllUsernames(), containsInAnyOrder( "Hanna", "Carol", "Mia" ) );
        assertThat( realm.getUsernamesForRole( "admin" ), containsInAnyOrder( "Mia" ) );
        assertThat( realm.getUsernamesForRole( "publisher" ), containsInAnyOrder( "Hanna", "Carol" ) );
    }

    @Test
    public void shouldReloadAuthFilesUntilValid() throws Exception
    {
        // we start with invalid auth file
        fs.addUserRoleFilePair(
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                        "Carol:SHA-256,FE0056C37E,A543:\n" +
                        "Mia:SHA-256,0E1FFFC23E,34"
                ,
                "THIS_WILL_NOT_BE_READ" );

        // now the roles file has non-existent users
        fs.addUserRoleFilePair(
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                        "Carol:SHA-256,FE0056C37E,A543:\n" +
                        "Mia:SHA-256,0E1FFFC23E,34A4:password_change_required\n"
                ,
                "admin:neo4j,Mao\n" +
                        "publisher:Hanna\n" );

        // finally valid files
        fs.addUserRoleFilePair(
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                        "Carol:SHA-256,FE0056C37E,A543:\n" +
                        "Mia:SHA-256,0E1FFFC23E,34A4:password_change_required\n"
                ,
                "admin:Mia\n" +
                        "publisher:Hanna,Carol\n" );

        jobScheduler.scheduledRunnable.run();

        assertThat( realm.getAllUsernames(), containsInAnyOrder( "Hanna", "Carol", "Mia" ) );
        assertThat( realm.getUsernamesForRole( "admin" ), containsInAnyOrder( "Mia" ) );
        assertThat( realm.getUsernamesForRole( "publisher" ), containsInAnyOrder( "Hanna", "Carol" ) );
    }

    @Test
    public void shouldEventuallyFailReloadAttempts()
    {
        // the roles file has non-existent users
        fs.addUserRoleFilePair(
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                "Carol:SHA-256,FE0056C37E,A543:\n" +
                "Mia:SHA-256,0E1FFFC23E,34A4:password_change_required\n"
                ,
                "admin:neo4j,Mao\n" +
                "publisher:Hanna\n" );

        // perma-broken auth file
        for ( int i = 0; i < LARGE_NUMBER - 1; i++ )
        {
            fs.addUserRoleFilePair(
                    "Hanna:SHA-256,FE0056C37E,A543:\n" +
                    "Carol:SHA-256,FE0056C37E,A543:\n" +
                    "Mia:SHA-256,0E1FFFC23E,34"
                    ,
                    "admin:Mia\n" +
                    "publisher:Hanna,Carol\n" );
        }

        try
        {
            jobScheduler.scheduledRunnable.run();
            fail( "Expected exception due to invalid auth file combo." );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString(
                    "Unable to load valid flat file repositories! Attempts failed with:" ) );
            File authFile = new File( "dbms/auth" );
            assertThat( e.getMessage(), containsString( "Failed to read authentication file: " + authFile ) );
            assertThat( e.getMessage(), containsString( "Role-auth file combination not valid" ) );
        }
    }

    static class TestJobScheduler extends Neo4jJobScheduler
    {
        Runnable scheduledRunnable;

        @Override
        public JobHandle schedule( Group group, Runnable r, long initialDelay, TimeUnit timeUnit )
        {
            this.scheduledRunnable = r;
            return null;
        }
    }

    private class EvilFileSystem extends DelegatingFileSystemAbstraction
    {
        private Queue<String> userStoreVersions = new LinkedList<>();
        private Queue<String> roleStoreVersions = new LinkedList<>();

        EvilFileSystem( FileSystemAbstraction delegate )
        {
            super( delegate );
        }

        void addUserRoleFilePair( String usersVersion, String rolesVersion )
        {
            userStoreVersions.add( usersVersion );
            roleStoreVersions.add( rolesVersion );
        }

        @Override
        public Reader openAsReader( File fileName, Charset charset ) throws IOException
        {
            if ( fileName.equals( userStoreFile ) )
            {
                return new CharArrayReader( userStoreVersions.remove().toCharArray() );
            }
            if ( fileName.equals( roleStoreFile ) )
            {
                if ( userStoreVersions.size() < roleStoreVersions.size() - 1 )
                {
                    roleStoreVersions.remove();
                }
                return new CharArrayReader( roleStoreVersions.remove().toCharArray() );
            }
            return super.openAsReader( fileName, charset );
        }

        @Override
        public long lastModifiedTime( File fileName ) throws IOException
        {
            if ( fileName.equals( userStoreFile ) )
            {
                return LARGE_NUMBER + 1 - userStoreVersions.size();
            }
            if ( fileName.equals( roleStoreFile ) )
            {
                return LARGE_NUMBER + 1 - roleStoreVersions.size();
            }
            return super.lastModifiedTime( fileName );
        }
    }
}
