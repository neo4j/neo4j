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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class AuthProceduresIT
{
    private static final String PWD_CHANGE = PASSWORD_CHANGE_REQUIRED.name().toLowerCase();

    private GraphDatabaseAPI db;
    private EphemeralFileSystemAbstraction fs;
    private BasicSystemGraphRealm authManager;
    private LoginContext admin;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup() throws InvalidAuthTokenException, IOException
    {
        fs = new EphemeralFileSystemAbstraction();
        db = (GraphDatabaseAPI) createGraphDatabase( fs );
        authManager = db.getDependencyResolver().resolveDependency( BasicSystemGraphRealm.class );
        admin = login( "neo4j", "neo4j" );
        admin.subject().setPasswordChangeNoLongerRequired();
    }

    @AfterEach
    void cleanup() throws Exception
    {
        managementService.shutdown();
        fs.close();
    }

    //---------- change password -----------

    @Test
    void shouldChangePassword() throws Throwable
    {

        // Given
        assertEmpty( admin, "CALL dbms.security.changePassword('abc')" );

        assert authManager.getUser( "neo4j" ).credentials().matchesPassword( UTF8.encode( "abc" ) );
    }

    @Test
    void shouldNotChangeOwnPasswordIfNewPasswordInvalid()
    {
        assertFail( admin, "CALL dbms.security.changePassword( '' )", "A password cannot be empty." );
        assertFail( admin, "CALL dbms.security.changePassword( 'neo4j' )", "Old password and new password cannot be the same." );
    }

    @Test
    void newUserShouldBeAbleToChangePassword() throws Throwable
    {
        // Given
        authManager.newUser( "andres", UTF8.encode( "banana" ), true );

        // Then
        assertEmpty( login("andres", "banana"), "CALL dbms.security.changePassword('abc')" );
    }

    @Test
    void newUserShouldNotBeAbleToCallOtherProcedures() throws Throwable
    {
        // Given
        authManager.newUser( "andres", UTF8.encode( "banana" ), true );
        LoginContext user = login("andres", "banana");

        // Then
        assertFail( user, "CALL dbms.procedures",
                "The credentials you provided were valid, but must be changed before you can use this instance." );
    }

    //---------- create user -----------

    @Test
    void shouldCreateUser() throws InvalidArgumentsException
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123', true)" );
        assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( true ) );
    }

    @Test
    void shouldCreateUserWithNoPasswordChange() throws InvalidArgumentsException
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123', false)" );
        assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( false ) );
    }

    @Test
    void shouldCreateUserWithDefault() throws InvalidArgumentsException
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123')" );
        assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( true ) );
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
                "The specified user 'neo4j' already exists" );
        assertFail( admin, "CALL dbms.security.createUser('neo4j', '', true)", "A password cannot be empty." );
    }

    //---------- delete user -----------

    @Test
    void shouldDeleteUser() throws Exception
    {
        authManager.newUser( "andres", UTF8.encode( "123" ), false );
        assertEmpty( admin, "CALL dbms.security.deleteUser('andres')" );
        try
        {
            authManager.getUser( "andres" );
            fail( "Andres should no longer exist, expected exception." );
        }
        catch ( InvalidArgumentsException e )
        {
            assertThat( e.getMessage(), containsString( "User 'andres' does not exist." ) );
        }
        catch ( Throwable t )
        {
            assertThat( t.getClass(), equalTo( InvalidArgumentsException.class ) );
        }
    }

    @Test
    void shouldNotDeleteNonExistentUser()
    {
        assertFail( admin, "CALL dbms.security.deleteUser('nonExistentUser')", "User 'nonExistentUser' does not exist" );
    }

    //---------- list users -----------

    @Test
    void shouldListUsers() throws Exception
    {
        authManager.newUser( "andres", UTF8.encode( "123" ), false );
        assertSuccess( admin, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIs( r, "username", "neo4j", "andres" ) );
    }

    @Test
    void shouldReturnUsersWithFlags() throws Exception
    {
        authManager.newUser( "andres", UTF8.encode( "123" ), false );
        Map<String,Object> expected = map(
                "neo4j", listOf( PWD_CHANGE ),
                "andres", listOf()
        );
        assertSuccess( admin, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "flags", expected ) );
    }

    @Test
    void shouldShowCurrentUser() throws Exception
    {
        assertSuccess( admin, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "flags", map( "neo4j", listOf( PWD_CHANGE ) ) ) );

        authManager.newUser( "andres", UTF8.encode( "123" ), false );
        LoginContext andres = login( "andres", "123" );
        assertSuccess( andres, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "flags", map( "andres", listOf() ) ) );
    }

    //---------- utility -----------

    private GraphDatabaseService createGraphDatabase( EphemeralFileSystemAbstraction fs ) throws IOException
    {
        removePreviousAuthFile();

        DatabaseManagementServiceBuilder graphDatabaseFactory = new TestDatabaseManagementServiceBuilder().setFileSystem( fs ).impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, true );

        managementService = graphDatabaseFactory.build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void removePreviousAuthFile() throws IOException
    {
        Path file = Paths.get( "target/test-data/" + DEFAULT_DATABASE_NAME + "/data/dbms/auth" );
        if ( Files.exists( file ) )
        {
            Files.delete( file );
        }
    }

    private LoginContext login( String username, String password ) throws InvalidAuthTokenException
    {
        return authManager.login( newBasicAuthToken( username, password ) );
    }

    private void assertEmpty( LoginContext subject, String query )
    {
        assertThat( execute( subject, query, r ->
                {
                    assert !r.hasNext();
                } ),
                equalTo( "" ) );
    }

    private void assertFail( LoginContext subject, String query, String partOfErrorMsg )
    {
        assertThat( execute( subject, query, r ->
                {
                    assert !r.hasNext();
                } ),
                containsString( partOfErrorMsg ) );
    }

    private void assertSuccess( LoginContext subject, String query,
            Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        assertThat(
                execute( subject, query, resultConsumer ),
                equalTo( "" ) );
    }

    private String execute( LoginContext subject, String query,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.implicit, subject ) )
        {
            resultConsumer.accept( db.execute( query ) );
            tx.commit();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    private List<Object> getObjectsAsList( ResourceIterator<Map<String,Object>> r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( Collectors.toList() );
    }

    private void assertKeyIs( ResourceIterator<Map<String,Object>> r, String key, String... items )
    {
        assertKeyIsArray( r, key, items );
    }

    private void assertKeyIsArray( ResourceIterator<Map<String,Object>> r, String key, String[] items )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertEquals( Arrays.asList( items ).size(), results.size() );
        assertThat( results, containsInAnyOrder( items ) );
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream(strs), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    private static List<String> listOf( String... values )
    {
        return Arrays.asList( values );
    }

    @SuppressWarnings( "unchecked" )
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
                assertThat( value, containsInAnyOrder( expectedValues.toArray() ) );
            }
            else
            {
                String value = objectValue.toString();
                String expectedValue = expected.get( key ).toString();
                assertEquals( value, expectedValue, String.format( "Wrong value for '%s', expected '%s', got '%s'", key, expectedValue, value ) );
            }
        }
    }
}
