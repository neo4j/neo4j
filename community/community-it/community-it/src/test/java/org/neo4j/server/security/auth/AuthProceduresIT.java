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
package org.neo4j.server.security.auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.deprecatedName;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class AuthProceduresIT
{
    private static final String PWD_CHANGE = PASSWORD_CHANGE_REQUIRED.name().toLowerCase();

    private GraphDatabaseAPI db;
    private GraphDatabaseAPI systemDb;
    private EphemeralFileSystemAbstraction fs;
    private BasicSystemGraphRealm authManager;
    private LoginContext admin;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup() throws InvalidAuthTokenException
    {
        fs = new EphemeralFileSystemAbstraction();
        DatabaseManagementServiceBuilder graphDatabaseFactory = new TestDatabaseManagementServiceBuilder().setFileSystem( fs ).impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, true );
        managementService = graphDatabaseFactory.build();

        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        systemDb = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        authManager = db.getDependencyResolver().resolveDependency( BasicSystemGraphRealm.class );

        assertSuccess( login( "neo4j", "neo4j" ), "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'temp'" );
        assertSuccess( login( "neo4j", "temp" ), "ALTER CURRENT USER SET PASSWORD FROM 'temp' TO 'neo4j'" );
        admin = login( "neo4j", "neo4j" );

    }

    @AfterEach
    void cleanup() throws Exception
    {
        managementService.shutdown();
        fs.close();
    }

    //---------- change password -----------

    @Test
    void shouldGiveErrorMessageForChangePasswordProcedure()
    {
        assertFail( admin, "CALL dbms.security.changePassword('abc123')", "This procedure is no longer available, use: 'ALTER CURRENT USER SET PASSWORD'" );
    }

    @Test
    void shouldGetDeprecatedNotificationForChangePasswordProcedure()
    {
        assertNotification( admin, "explain CALL dbms.security.changePassword('abc123')",
                deprecatedProcedureNotification( "dbms.security.changePassword", "Administration command: ALTER CURRENT USER SET PASSWORD" ) );
    }

    @Test
    void shouldChangePassword() throws Throwable
    {
        // Given
        assertSuccess( admin, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'abc'" );

        assertThat( login( "neo4j", "neo4j" ).subject().getAuthenticationResult() ).isEqualTo( FAILURE );
        assertThat( login( "neo4j", "abc" ).subject().getAuthenticationResult() ).isEqualTo( SUCCESS );
    }

    @Test
    void shouldNotChangeOwnPasswordIfNewPasswordInvalid()
    {
        assertFail( admin, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO ''", "A password cannot be empty." );
        assertFail( admin, "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'neo4j'", "Old password and new password cannot be the same." );
    }

    @Test
    void newUserShouldBeAbleToChangePassword() throws Throwable
    {
        // Given
        assertSuccess( LoginContext.AUTH_DISABLED, "CREATE USER andres SET PASSWORD 'banana'" );

        // Then
        assertSuccess( login( "andres", "banana" ), "ALTER CURRENT USER SET PASSWORD FROM 'banana' TO 'abc'" );
    }

    @Test
    void newUserShouldNotBeAbleToCallOtherProcedures() throws Throwable
    {
        // Given
        assertSuccess( admin, "CREATE USER andres SET PASSWORD 'banana'" );
        LoginContext user = login( "andres", "banana" );

        // Then
        assertThat( execute( user, "CALL dbms.procedures", r ->
        {
            assertFalse( r.hasNext() );
        } ) ).contains( "The credentials you provided were valid, but must be changed before you can use this instance." );
    }

    //---------- create user -----------

    @Test
    void shouldCreateUser()
    {
        assertSuccess( admin, "CALL dbms.security.createUser('andres', '123', true)" );

        assertSuccess( admin, "SHOW USERS", r -> {
            Set<Map<String,Object>> users = r.stream().collect( Collectors.toSet() );
            assertTrue( users.containsAll( Set.of(
                    Map.of( "user", "neo4j", "passwordChangeRequired", false ),
                    Map.of( "user", "andres", "passwordChangeRequired", true )
            ) ) );
        } );
    }

    @Test
    void shouldCreateUserWithNoPasswordChange()
    {
        assertSuccess( admin, "CALL dbms.security.createUser('andres', '123', false)" );
        assertSuccess( admin, "SHOW USERS", r -> {
            Set<Map<String,Object>> users = r.stream().collect( Collectors.toSet() );
            assertTrue( users.containsAll( Set.of(
                    Map.of( "user", "neo4j", "passwordChangeRequired", false ),
                    Map.of( "user", "andres", "passwordChangeRequired", false )
            ) ) );
        } );
    }

    @Test
    void shouldCreateUserWithDefault()
    {
        assertSuccess( admin, "CALL dbms.security.createUser('andres', '123')" );
        assertSuccess( admin, "SHOW USERS", r -> {
            Set<Map<String,Object>> users = r.stream().collect( Collectors.toSet() );
            assertTrue( users.containsAll( Set.of(
                    Map.of( "user", "neo4j", "passwordChangeRequired", false ),
                    Map.of( "user", "andres", "passwordChangeRequired", true )
            ) ) );
        } );
    }

    @Test
    void shouldGetDeprecatedNotificationForCreateUser()
    {
        assertNotification( admin, "explain CALL dbms.security.createUser('andres', '123')",
                deprecatedProcedureNotification( "dbms.security.createUser", "Administration command: CREATE USER" ) );
    }

    @Test
    void shouldNotCreateUserIfInvalidUsername()
    {
        assertFail( admin, "CALL dbms.security.createUser('', '1234', true)", "The provided username is empty." );
        assertFail( admin, "CALL dbms.security.createUser(',!', '1234', true)",
                "Username ',!' contains illegal characters." );
        assertFail( admin, "CALL dbms.security.createUser(':ss!', '', true)", "Username ':ss!' contains illegal " +
                "characters." );
    }

    @Test
    void shouldNotCreateUserIfInvalidPassword()
    {
        assertFail( admin, "CALL dbms.security.createUser('andres', '', true)", "A password cannot be empty." );
    }

    @Test
    void shouldNotCreateExistingUser()
    {
        assertFail( admin, "CALL dbms.security.createUser('neo4j', '1234', true)",
                "Failed to create the specified user 'neo4j': User already exists." );
        assertFail( admin, "CALL dbms.security.createUser('neo4j', '', true)", "A password cannot be empty." );
    }

    //---------- delete user -----------

    @Test
    void shouldDeleteUser()
    {
        // GIVEN
        assertSuccess( admin, "CREATE USER andres SET PASSWORD '123' CHANGE NOT REQUIRED" );

        // WHEN
        assertSuccess( admin, "CALL dbms.security.deleteUser('andres')" );

        // THEN
        assertSuccess( admin, "SHOW USERS", r -> {
            Set<Map<String,Object>> users = r.stream().collect( Collectors.toSet() );
            assertTrue( users.contains( Map.of( "user", "neo4j", "passwordChangeRequired", false ) ) );
        } );
    }

    @Test
    void shouldGetDeprecatedNotificationForDeleteUser()
    {
        assertNotification( admin, "explain CALL dbms.security.deleteUser('andres')",
                deprecatedProcedureNotification( "dbms.security.deleteUser", "Administration command: DROP USER" ) );
    }

    @Test
    void shouldNotDeleteNonExistentUser()
    {
        assertFail( admin, "CALL dbms.security.deleteUser('nonExistentUser')",
                "Failed to delete the specified user 'nonExistentUser': User does not exist." );
    }

    //---------- list users -----------

    @Test
    void shouldListUsers()
    {
        assertSuccess( admin, "CREATE USER andres SET PASSWORD '123' CHANGE NOT REQUIRED" );
        assertSuccess( admin, "CALL dbms.security.listUsers() YIELD username", r ->
        {
            String[] items = new String[]{"neo4j", "andres"};
            List<Object> results = r.stream().map( s -> s.get( "username" ) ).collect( Collectors.toList() );
            assertEquals( Arrays.asList( items ).size(), results.size() );
            assertThat( results ).contains( items );
        } );
    }

    @Test
    void shouldReturnUsersWithFlags()
    {
        assertSuccess( admin, "CREATE USER andres SET PASSWORD '123'" );
        Map<String,Object> expected = map(
                "neo4j", emptyList(),
                "andres", List.of( PWD_CHANGE )
        );
        assertSuccess( admin, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "flags", expected ) );
    }

    @Test
    void shouldShowCurrentUser()
    {
        assertThat( execute( admin, "CALL dbms.showCurrentUser()", r -> assertKeyIsMap( r, "username", "flags", map( "neo4j", emptyList() ) ) ) ).isEqualTo(
                "" );
    }

    @Test
    void shouldGetDeprecatedNotificationForListUsers()
    {
        assertNotification( admin, "explain CALL dbms.security.listUsers",
                deprecatedProcedureNotification( "dbms.security.listUsers", "Administration command: SHOW USERS" ) );
    }

    //---------- utility -----------

    private LoginContext login( String username, String password ) throws InvalidAuthTokenException
    {
        return authManager.login( newBasicAuthToken( username, password ) );
    }

    private void assertSuccess( LoginContext subject, String query )
    {
        assertSuccess( subject, query, r ->
        {
            assert !r.hasNext();
        } );
    }

    private void assertNotification( LoginContext subject, String query, Notification wantedNotification )
    {
        try ( Transaction tx = systemDb.beginTransaction( KernelTransaction.Type.IMPLICIT, subject ) )
        {
            Result result = tx.execute( query );

            Iterator<Notification> givenNotifications = result.getNotifications().iterator();
            if ( givenNotifications.hasNext() )
            {
                assertEquals( wantedNotification, givenNotifications.next() ); // only checks first notification
            }
            else
            {
                fail( "Expected notifications from '" + query + "'" );
            }

            tx.commit();
        }
    }

    private void assertSuccess( LoginContext subject, String query, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        try ( Transaction tx = systemDb.beginTransaction( KernelTransaction.Type.IMPLICIT, subject ) )
        {
            Result result = tx.execute( query );
            resultConsumer.accept( result );
            tx.commit();
        }
    }

    private void assertFail( LoginContext subject, String query, String partOfErrorMsg )
    {
        Consumer<ResourceIterator<Map<String,Object>>> resultConsumer = row ->
        {
            assert !row.hasNext();
        };
        try ( Transaction tx = systemDb.beginTransaction( KernelTransaction.Type.IMPLICIT, subject ) )
        {
            Result result = tx.execute( query );
            resultConsumer.accept( result );
            tx.commit();
            fail( "Expected query to fail" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage() ).contains( partOfErrorMsg );
        }
    }

    private String execute( LoginContext subject, String query,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.IMPLICIT, subject ) )
        {
            resultConsumer.accept( tx.execute( query ) );
            tx.commit();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream( strs ), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    @SuppressWarnings( {"unchecked", "SameParameterValue"} )
    static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey, Map<String,Object> expected )
    {
        List<Map<String, Object>> result = r.stream().collect( Collectors.toList() );

        assertEquals( expected.size(), result.size(), "Results for should have size " + expected.size() + " but was " + result.size() );

        for ( Map<String, Object> row : result )
        {
            String key = (String) row.get( keyKey );
            assertTrue( expected.containsKey( key ), "Unexpected key '" + key + "'" );

            assertTrue( row.containsKey( valueKey ), "Value key '" + valueKey + "' not found in results" );
            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof List )
            {
                List<String> value = (List<String>) objectValue;
                List<String> expectedValues = (List<String>) expected.get( key );
                assertEquals( value.size(), expectedValues.size(),
                        "Results for '" + key + "' should have size " + expectedValues.size() + " but was " +
                                value.size() );
                assertThat( value ).containsAll( expectedValues );
            }
            else
            {
                String value = objectValue.toString();
                String expectedValue = expected.get( key ).toString();
                assertEquals( value, expectedValue, String.format( "Wrong value for '%s', expected '%s', got '%s'", key, expectedValue, value ) );
            }
        }
    }

    private Notification deprecatedProcedureNotification( String oldName, String newName )
    {
        return DEPRECATED_PROCEDURE.notification( new InputPosition( 8, 1, 9 ), deprecatedName( oldName, newName ) );
    }
}
