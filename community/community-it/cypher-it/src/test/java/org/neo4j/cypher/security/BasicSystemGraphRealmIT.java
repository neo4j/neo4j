/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.server.security.systemgraph.UserSecurityGraphInitializer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFails;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFailsWithTooManyAttempts;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.createUser;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@TestDirectoryExtension
class BasicSystemGraphRealmIT
{
    private BasicSystemGraphRealmTestHelper.TestDatabaseManager dbManager;
    private SystemGraphRealmHelper realmHelper;
    private SecureHasher secureHasher;
    private Config defaultConfig;

    @Inject
    private TestDirectory testDirectory;

    private UserRepository oldUsers;
    private UserRepository initialPassword;

    @BeforeEach
    void setUp()
    {
        dbManager = new BasicSystemGraphRealmTestHelper.TestDatabaseManager( testDirectory );
        secureHasher = new SecureHasher();
        realmHelper = new SystemGraphRealmHelper( dbManager, secureHasher );
        defaultConfig = Config.defaults();
        oldUsers = new InMemoryUserRepository();
        initialPassword = new InMemoryUserRepository();
    }

    @AfterEach
    void tearDown()
    {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        startSystemGraphRealm();
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true );
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "123", false ) );
        startSystemGraphRealm();
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "123" );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = startSystemGraphRealm();

        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true );

        realm.stop();

        initialPassword.create( createUser( INITIAL_USER_NAME, "abc", false ) );

        // When
        realm.start();

        // Then
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "abc" );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = startSystemGraphRealm();
        realm.stop();
        initialPassword.create( createUser( INITIAL_USER_NAME, "neo4j2", false ) );
        realm.start();
        realm.stop();

        // When
        initialPassword.clear();
        initialPassword.create( createUser( INITIAL_USER_NAME, "abc", false ) );
        realm.start();

        // Then
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "neo4j2" );
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, "abc" );
    }

    @Test
    void shouldNotAddInitialUserIfUsersExist() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "123", false ) );
        oldUsers.create( createUser( "oldUser", "321", false ) );
        startSystemGraphRealm();

        User initUser;
        try
        {
            initUser = realmHelper.getUser( INITIAL_USER_NAME );
        }
        catch ( InvalidArgumentsException | FormatException e )
        {
            initUser = null;
        }

        assertNull( initUser );
        assertAuthenticationSucceeds( realmHelper, "oldUser", "321" );
    }

    @Test
    void shouldNotUpdateUserIfInitialUserExist() throws Throwable
    {
        oldUsers.create( createUser( INITIAL_USER_NAME, "oldPassword", true ) );
        initialPassword.create( createUser( INITIAL_USER_NAME, "newPassword", false ) );

        startSystemGraphRealm();
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "oldPassword", true );
    }

    @Test
    void shouldRateLimitAuthentication() throws Throwable
    {
        int maxFailedAttempts = Config.defaults().get( GraphDatabaseSettings.auth_max_failed_attempts );
        oldUsers.create( createUser( "alice", "correct", false ) );
        oldUsers.create( createUser( "bob", "password", false ) );
        BasicSystemGraphRealm realm = startSystemGraphRealm();

        // First make sure one of the users will have a cached successful authentication result for variation
        assertAuthenticationSucceeds( realmHelper, "alice", "correct" );

        assertAuthenticationFailsWithTooManyAttempts( realm, "alice", "bad", maxFailedAttempts + 1 );
        assertAuthenticationFailsWithTooManyAttempts( realm, "bob", "worse", maxFailedAttempts + 1 );
    }

    @Test
    void shouldHandleCustomDefaultDatabase() throws Throwable
    {
        defaultConfig.set( default_database, "foo" );
        oldUsers.create( createUser( "alice", "foo", false ) );

        startSystemGraphRealm();
        assertAuthenticationSucceeds( realmHelper, "alice", "foo" );
    }

    @Test
    void shouldHandleSwitchOfDefaultDatabase() throws Throwable
    {
        oldUsers.create( createUser( "alice", "bar", false ) );
        BasicSystemGraphRealm realm = startSystemGraphRealm();

        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );

        realm.stop();

        // Set a new database foo to default db in config
        defaultConfig.set( default_database, "foo" );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );

        realm.stop();

        // Switch back default db to 'neo4j'
        defaultConfig.set( default_database, DEFAULT_DATABASE_NAME );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );
    }

    private BasicSystemGraphRealm startSystemGraphRealm() throws Exception
    {
        Config config = Config.defaults( DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ).toPath() );
        DefaultSystemGraphInitializer systemGraphInitializer = new DefaultSystemGraphInitializer( dbManager, config );

        UserSecurityGraphInitializer securityGraphInitializer =
                new UserSecurityGraphInitializer( dbManager, systemGraphInitializer, Mockito.mock( Log.class ), oldUsers, initialPassword, secureHasher );

        RateLimitedAuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( Clock.systemUTC(), config );
        BasicSystemGraphRealm realm = new BasicSystemGraphRealm( securityGraphInitializer, realmHelper, authStrategy );
        realm.start();
        return realm;
    }
}
