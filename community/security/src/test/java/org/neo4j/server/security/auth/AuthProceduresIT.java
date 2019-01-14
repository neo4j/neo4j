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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;

public class AuthProceduresIT
{
    private static final String PWD_CHANGE = PASSWORD_CHANGE_REQUIRED.name().toLowerCase();

    protected GraphDatabaseAPI db;
    private EphemeralFileSystemAbstraction fs;
    private BasicAuthManager authManager;
    private LoginContext admin;

    @Before
    public void setup() throws InvalidAuthTokenException, IOException
    {
        fs = new EphemeralFileSystemAbstraction();
        db = (GraphDatabaseAPI) createGraphDatabase( fs );
        authManager = db.getDependencyResolver().resolveDependency( BasicAuthManager.class );
        admin = login( "neo4j", "neo4j" );
        admin.subject().setPasswordChangeNoLongerRequired();
    }

    @After
    public void cleanup() throws Exception
    {
        db.shutdown();
        fs.close();
    }

    //---------- change password -----------

    @Test
    public void shouldChangePassword() throws Throwable
    {

        // Given
        assertEmpty( admin, "CALL dbms.changePassword('abc')" );

        assert authManager.getUser( "neo4j" ).credentials().matchesPassword( "abc" );
    }

    @Test
    public void shouldNotChangeOwnPasswordIfNewPasswordInvalid()
    {
        assertFail( admin, "CALL dbms.changePassword( '' )", "A password cannot be empty." );
        assertFail( admin, "CALL dbms.changePassword( 'neo4j' )", "Old password and new password cannot be the same." );
    }

    @Test
    public void newUserShouldBeAbleToChangePassword() throws Throwable
    {
        // Given
        authManager.newUser( "andres", "banana", true );

        // Then
        assertEmpty( login("andres", "banana"), "CALL dbms.changePassword('abc')" );
    }

    @Test
    public void newUserShouldNotBeAbleToCallOtherProcedures() throws Throwable
    {
        // Given
        authManager.newUser( "andres", "banana", true );
        LoginContext user = login("andres", "banana");

        // Then
        assertFail( user, "CALL dbms.procedures",
                "The credentials you provided were valid, but must be changed before you can use this instance." );
    }

    //---------- create user -----------

    @Test
    public void shouldCreateUser()
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123', true)" );
        try
        {
            assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( true ) );
        }
        catch ( Throwable t )
        {
            fail( "Expected no exception!" );
        }
    }

    @Test
    public void shouldCreateUserWithNoPasswordChange()
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123', false)" );
        try
        {
            assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( false ) );
        }
        catch ( Throwable t )
        {
            fail( "Expected no exception!" );
        }
    }

    @Test
    public void shouldCreateUserWithDefault()
    {
        assertEmpty( admin, "CALL dbms.security.createUser('andres', '123')" );
        try
        {
            assertThat( authManager.getUser( "andres" ).passwordChangeRequired(), equalTo( true ) );
        }
        catch ( Throwable t )
        {
            fail( "Expected no exception!" );
        }
    }

    @Test
    public void shouldNotCreateUserIfInvalidUsername()
    {
        assertFail( admin, "CALL dbms.security.createUser('', '1234', true)", "The provided username is empty." );
        assertFail( admin, "CALL dbms.security.createUser(',!', '1234', true)",
                "Username ',!' contains illegal characters." );
        assertFail( admin, "CALL dbms.security.createUser(':ss!', '', true)", "Username ':ss!' contains illegal " +
                "characters." );
    }

    @Test
    public void shouldNotCreateUserIfInvalidPassword()
    {
        assertFail( admin, "CALL dbms.security.createUser('andres', '', true)", "A password cannot be empty." );
    }

    @Test
    public void shouldNotCreateExistingUser()
    {
        assertFail( admin, "CALL dbms.security.createUser('neo4j', '1234', true)",
                "The specified user 'neo4j' already exists" );
        assertFail( admin, "CALL dbms.security.createUser('neo4j', '', true)", "A password cannot be empty." );
    }

    //---------- delete user -----------

    @Test
    public void shouldDeleteUser() throws Exception
    {
        authManager.newUser( "andres", "123", false );
        assertEmpty( admin, "CALL dbms.security.deleteUser('andres')" );
        try
        {
            authManager.getUser( "andres" );
            fail("Andres should no longer exist, expected exception.");
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
    public void shouldNotDeleteNonExistentUser()
    {
        assertFail( admin, "CALL dbms.security.deleteUser('nonExistentUser')", "User 'nonExistentUser' does not exist" );
    }

    //---------- list users -----------

    @Test
    public void shouldListUsers() throws Exception
    {
        authManager.newUser( "andres", "123", false );
        assertSuccess( admin, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIs( r, "username", "neo4j", "andres" ) );
    }

    @Test
    public void shouldReturnUsersWithFlags() throws Exception
    {
        authManager.newUser( "andres", "123", false );
        Map<String,Object> expected = map(
                "neo4j", listOf( PWD_CHANGE ),
                "andres", listOf()
        );
        assertSuccess( admin, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "flags", expected ) );
    }

    @Test
    public void shouldShowCurrentUser() throws Exception
    {
        assertSuccess( admin, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "flags", map( "neo4j", listOf( PWD_CHANGE ) ) ) );

        authManager.newUser( "andres", "123", false );
        LoginContext andres = login( "andres", "123" );
        assertSuccess( andres, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "flags", map( "andres", listOf() ) ) );
    }

    //---------- utility -----------

    private GraphDatabaseService createGraphDatabase( EphemeralFileSystemAbstraction fs ) throws IOException
    {
        removePreviousAuthFile();
        Map<Setting<?>, String> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.auth_enabled, "true" );

        TestGraphDatabaseBuilder graphDatabaseFactory = (TestGraphDatabaseBuilder) new TestGraphDatabaseFactory()
                .setFileSystem( fs )
            .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.auth_enabled, "true" );

        return graphDatabaseFactory.newGraphDatabase();
    }

    private void removePreviousAuthFile() throws IOException
    {
        Path file = Paths.get( "target/test-data/impermanent-db/data/dbms/auth" );
        if ( Files.exists( file ) )
        {
            Files.delete( file );
        }
    }

    private LoginContext login( String username, String password ) throws InvalidAuthTokenException
    {
        return authManager.login( SecurityTestUtils.authToken( username, password ) );
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
            tx.success();
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
        Assert.assertThat( results, containsInAnyOrder( items ) );
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream(strs), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    private List<String> listOf( String... values )
    {
        return Stream.of( values ).collect( Collectors.toList() );
    }

    @SuppressWarnings( "unchecked" )
    public static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            Map<String,Object> expected )
    {
        List<Map<String, Object>> result = r.stream().collect( Collectors.toList() );

        assertEquals( "Results for should have size " + expected.size() + " but was " + result.size(),
                expected.size(), result.size() );

        for ( Map<String, Object> row : result )
        {
            String key = (String) row.get( keyKey );
            assertTrue( "Unexpected key '" + key + "'", expected.containsKey( key ) );

            assertTrue( "Value key '" + valueKey + "' not found in results", row.containsKey( valueKey ) );
            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof List )
            {
                List<String> value = (List<String>) objectValue;
                List<String> expectedValues = (List<String>) expected.get( key );
                assertEquals( "Results for '" + key + "' should have size " + expectedValues.size() + " but was " +
                        value.size(), value.size(), expectedValues.size() );
                assertThat( value, containsInAnyOrder( expectedValues.toArray() ) );
            }
            else
            {
                String value = objectValue.toString();
                String expectedValue = expected.get( key ).toString();
                assertTrue(
                        String.format( "Wrong value for '%s', expected '%s', got '%s'", key, expectedValue, value),
                        value.equals( expectedValue )
                );
            }
        }
    }
}
