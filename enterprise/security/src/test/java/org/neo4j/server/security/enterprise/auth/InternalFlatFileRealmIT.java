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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.DelegateFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class InternalFlatFileRealmIT
{
    File userStoreFile;
    File roleStoreFile;

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    TestScheduledExecutorService executor = new TestScheduledExecutorService( 1 );
    LogProvider logProvider = NullLogProvider.getInstance();
    InternalFlatFileRealm realm;

    @Before
    public void setup() throws Throwable
    {
        userStoreFile = new File( "dbms", "auth" );
        roleStoreFile = new File( "dbms", "roles" );
        final UserRepository userRepository = new FileUserRepository( fsRule.get(), userStoreFile, logProvider );
        final RoleRepository roleRepository = new FileRoleRepository( fsRule.get(), roleStoreFile, logProvider );
        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        AuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 );

        realm = new InternalFlatFileRealm( userRepository, roleRepository, passwordPolicy, authenticationStrategy,
                        true, true, executor );
        realm.init();
        realm.start();
    }

    @After
    public void teardown() throws Throwable
    {
        realm.shutdown();
    }

    @Test
    public void shouldReloadAuthFiles() throws Exception
    {
        overwrite( userStoreFile,
                "Hanna:SHA-256,FE0056C37E,A543:\n" +
                "Carol:SHA-256,FE0056C37E,A543:\n" +
                "Mia:SHA-256,0E1FFFC23E,34A4:password_change_required\n"  );

        overwrite( roleStoreFile,
                "admin:Mia\n" +
                "publisher:Hanna,Carol\n" );

        executor.scheduledRunnable.run();

        assertThat( realm.getAllUsernames(), containsInAnyOrder( "Hanna", "Carol", "Mia" ) );
        assertThat( realm.getUsernamesForRole( "admin" ), containsInAnyOrder( "Mia" ) );
        assertThat( realm.getUsernamesForRole( "publisher" ), containsInAnyOrder( "Hanna", "Carol" ) );
    }

    protected void overwrite( File file, String newContents ) throws IOException
    {
        FileSystemAbstraction fs = fsRule.get();

        fs.deleteFile( file );
        Writer w = fs.openAsWriter( file, Charset.defaultCharset(), false );
        w.write( newContents );
        w.close();
    }

    class TestScheduledExecutorService extends ScheduledThreadPoolExecutor
    {
        Runnable scheduledRunnable;

        public TestScheduledExecutorService( int corePoolSize )
        {
            super( corePoolSize );
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate( Runnable r, long initialDelay, long delay, TimeUnit timeUnit )
        {
            this.scheduledRunnable = r;
            return null;
        }
    }
}
