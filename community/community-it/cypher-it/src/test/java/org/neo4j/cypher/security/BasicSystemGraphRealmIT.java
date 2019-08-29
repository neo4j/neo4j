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
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFailsWithTooManyAttempts;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.auth.BasicSystemGraphRealmTest.clearedPasswordWithSameLengthAs;
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

        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserIfNoneExist() throws Throwable
    {
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().initialUser( "123", false );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
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
        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory, defaultConfig );

        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );

        realm.stop();

        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given started and stopped database
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory, defaultConfig );
        realm.setUserPassword( INITIAL_USER_NAME, UTF8.encode( "neo4j2" ), false );
        realm.stop();
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertFalse( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j2") ) );
    }

    @Test
    void shouldNotAddInitialUserIfUsersExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm(
                new BasicImportOptionsBuilder().initialUser( "123", false ).migrateUser( "oldUser", "321", false ), dbManager, defaultConfig );

        final User initUser = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNull( initUser );

        final User oldUser = realm.silentlyGetUser( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( password( "321" ) ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    void shouldNotUpdateUserIfInitialUserExist() throws Throwable
    {
        BasicImportOptionsBuilder importOptions =
                new BasicImportOptionsBuilder().initialUser( "newPassword", false ).migrateUser( INITIAL_USER_NAME, "oldPassword", true );

        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        final User oldUser = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( oldUser );
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
    void shouldClearPasswordOnNewUser() throws Throwable
    {
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        byte[] password = password( "jake" );

        // When
        realm.newUser( "jake", password, true );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnNewUserAlreadyExists() throws Throwable
    {
        // Given
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        realm.newUser( "jake", password( "jake" ), true );
        byte[] password = password( "abc123" );

        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.newUser( "jake", password, true ) );
        assertThat( exception.getMessage(), equalTo( "The specified user 'jake' already exists." ) );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPassword() throws Throwable
    {
        // Given
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        realm.newUser( "jake", password( "abc123" ), false );

        byte[] newPassword = password( "jake" );

        // When
        realm.setUserPassword( "jake", newPassword, false );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPasswordWithInvalidPassword() throws Throwable
    {
        // Given
        BasicImportOptionsBuilder importOptions = new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( importOptions, dbManager, defaultConfig );

        realm.newUser( "jake", password( "jake" ), false );
        byte[] newPassword = password( "jake" );

        // When
        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.setUserPassword( "jake", newPassword, false ) );
        assertThat( exception.getMessage(), equalTo( "Old password and new password cannot be the same." ) );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
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
        SetInitialPasswordCommand command =
                new SetInitialPasswordCommand( new ExecutionContext( testDirectory.directory().toPath(), testDirectory.directory( "conf" ).toPath(),
                        mock( PrintStream.class ), mock( PrintStream.class ), testDirectory.getFileSystem() ) );

        CommandLine.populateCommand( command, SIMULATED_INITIAL_PASSWORD );
        command.execute();
    }

    public static final String SIMULATED_INITIAL_PASSWORD = "neo4j1";
}
