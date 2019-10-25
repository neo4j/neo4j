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
import picocli.CommandLine;

import java.io.PrintStream;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFailsWithTooManyAttempts;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

@TestDirectoryExtension
public class BasicSystemGraphRealmIT
{
    private BasicSystemGraphRealmTestHelper.TestDatabaseManager dbManager;
    private Config defaultConfig;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        dbManager = new BasicSystemGraphRealmTestHelper.TestDatabaseManager( testDirectory );
        defaultConfig = Config.defaults();
    }

    @AfterEach
    void tearDown()
    {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder(), dbManager, defaultConfig );

        final User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserIfNoneExist() throws Throwable
    {
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().initialUser( "123", false );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        final User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertTrue( user.credentials().matchesPassword( password("123") ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable
    {
        // Given
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory, defaultConfig );

        // Then
        final User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory, defaultConfig );

        User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );

        realm.stop();

        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory, defaultConfig );
        realm.stop();
        simulateSetInitialPasswordCommand( testDirectory, "neo4j2" );
        realm.start();
        realm.stop();

        // When
        simulateSetInitialPasswordCommand(testDirectory);
        realm.start();

        // Then
        User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertFalse( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j2") ) );
    }

    @Test
    void shouldNotAddInitialUserIfUsersExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm(
                new BasicImportOptionsBuilder().initialUser( "123", false ).migrateUser( "oldUser", "321", false ), dbManager, defaultConfig );

        final User initUser = silentlyGetUser( realm, INITIAL_USER_NAME );
        assertNull( initUser );

        final User oldUser = getExistingUser( realm, "oldUser" );
        assertTrue( oldUser.credentials().matchesPassword( password( "321" ) ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    void shouldNotUpdateUserIfInitialUserExist() throws Throwable
    {
        BasicImportOptionsBuilder importOptions =
                new BasicImportOptionsBuilder().initialUser( "newPassword", false ).migrateUser( INITIAL_USER_NAME, "oldPassword", true );

        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        final User oldUser = getExistingUser( realm, INITIAL_USER_NAME );
        assertTrue( oldUser.credentials().matchesPassword( password( "oldPassword" ) ) );
        assertTrue( oldUser.passwordChangeRequired() );
    }

    @Test
    void shouldRateLimitAuthentication() throws Throwable
    {
        int maxFailedAttempts = Config.defaults().get( GraphDatabaseSettings.auth_max_failed_attempts );
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        // First make sure one of the users will have a cached successful authentication result for variation
        assertAuthenticationSucceeds( realm, "alice" );

        assertAuthenticationFailsWithTooManyAttempts( realm, "alice", maxFailedAttempts + 1 );
        assertAuthenticationFailsWithTooManyAttempts( realm, "bob", maxFailedAttempts + 1 );
    }

    @Test
    void shouldHandleCustomDefaultDatabase() throws Throwable
    {
        defaultConfig.set( default_database, "foo" );

        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        assertAuthenticationSucceeds( realm, "alice" );
    }

    @Test
    void shouldHandleSwitchOfDefaultDatabase() throws Throwable
    {
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        assertAuthenticationSucceeds( realm, "alice" );

        realm.stop();

        // Set a new database foo to default db in config
        defaultConfig.set( default_database, "foo" );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );

        realm.stop();

        // Switch back default db to 'neo4j'
        defaultConfig.set( default_database, DEFAULT_DATABASE_NAME );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );
    }

    public static void simulateSetInitialPasswordCommand( TestDirectory testDirectory )
    {
        simulateSetInitialPasswordCommand( testDirectory, SIMULATED_INITIAL_PASSWORD );
    }

    public static void simulateSetInitialPasswordCommand( TestDirectory testDirectory, String newPwd )
    {

        SetInitialPasswordCommand command =
                new SetInitialPasswordCommand( new ExecutionContext( testDirectory.homeDir().toPath(), testDirectory.directory( "conf" ).toPath(),
                        mock( PrintStream.class ), mock( PrintStream.class ), testDirectory.getFileSystem() ) );

        CommandLine.populateCommand( command, newPwd );
        command.execute();
    }

    public static final String SIMULATED_INITIAL_PASSWORD = "neo4j1";

    public static User getExistingUser( BasicSystemGraphRealm realm, String username )
    {
        User user = silentlyGetUser( realm, username );
        assertNotNull( user );
        return user;
    }

    private static User silentlyGetUser( BasicSystemGraphRealm realm, String username )
    {
        try
        {
            return realm.getUser( username );
        }
        catch ( InvalidArgumentsException | FormatException e )
        {
            return null;
        }
    }
}
