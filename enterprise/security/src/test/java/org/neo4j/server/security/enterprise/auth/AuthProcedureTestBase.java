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
import org.junit.Assert;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.security.AuthorizationViolationException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

abstract class AuthProcedureTestBase<S>
{
    final String EMPTY_ROLE = "empty";

    S adminSubject;
    S schemaSubject;
    S writeSubject;
    S readSubject;
    S pwdSubject;
    S noneSubject;

    String[] initialUsers = { "adminSubject", "readSubject", "schemaSubject",
        "readWriteSubject", "pwdSubject", "noneSubject", "neo4j" };
    String[] initialRoles = { ADMIN, ARCHITECT, PUBLISHER, READER, EMPTY_ROLE };

    protected EnterpriseUserManager userManager;

    protected NeoInteractionLevel<S> neo;

    @Before
    public void setUp() throws Throwable
    {
        neo = setUpNeoServer();
        userManager = neo.getManager();

        userManager.newUser( "noneSubject", "abc", false );
        userManager.newUser( "pwdSubject", "abc", true );
        userManager.newUser( "adminSubject", "abc", false );
        userManager.newUser( "schemaSubject", "abc", false );
        userManager.newUser( "readWriteSubject", "abc", false );
        userManager.newUser( "readSubject", "123", false );
        // Currently admin role is created by default
        userManager.addUserToRole( "adminSubject", ADMIN );
        userManager.addUserToRole( "schemaSubject", ARCHITECT );
        userManager.addUserToRole( "readWriteSubject", PUBLISHER );
        userManager.addUserToRole( "readSubject", READER );
        userManager.newRole( EMPTY_ROLE );
        noneSubject = neo.login( "noneSubject", "abc" );
        pwdSubject = neo.login( "pwdSubject", "abc" );
        readSubject = neo.login( "readSubject", "123" );
        writeSubject = neo.login( "readWriteSubject", "abc" );
        schemaSubject = neo.login( "schemaSubject", "abc" );
        adminSubject = neo.login( "adminSubject", "abc" );
        executeQuery( writeSubject, "UNWIND range(0,2) AS number CREATE (:Node {number:number})" );
    }

    abstract NeoInteractionLevel<S> setUpNeoServer() throws Throwable;

    @After
    public void tearDown() throws Throwable
    {
        neo.tearDown();
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream(strs), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    protected List<String> listOf( String... values )
    {
        return Stream.of( values ).collect( Collectors.toList() );
    }

    //------------- Helper functions---------------

    void testSuccessfulRead( S subject, int count )
    {
        testCallCount( subject, "MATCH (n) RETURN n", null, count );
    }

    void testFailRead( S subject, int count )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "MATCH (n) RETURN n",
                AuthorizationViolationException.class, "Read operations are not allowed" );
    }

    void testSuccessfulWrite( S subject )
    {
        testCallEmpty( subject, "CREATE (:Node)" );
    }

    void testFailWrite( S subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "CREATE (:Node)",
                AuthorizationViolationException.class, "Write operations are not allowed" );
    }

    void testSuccessfulSchema( S subject )
    {
        testCallEmpty( subject, "CREATE INDEX ON :Node(number)" );
    }

    void testFailSchema( S subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "CREATE INDEX ON :Node(number)",
                AuthorizationViolationException.class, "Schema operations are not allowed" );
    }

    void testFailCreateUser( S subject )
    {
        testCallFail( subject, "CALL dbms.createUser('Craig', 'foo', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.createUser('Craig', '', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.createUser('', 'foo', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    void testFailAddUserToRole( S subject )
    {
        testCallFail( subject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    void testFailRemoveUserFromRole( S subject )
    {
        testCallFail( subject, "CALL dbms.removeUserFromRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    void testFailDeleteUser( S subject )
    {
        testCallFail( subject, "CALL dbms.deleteUser('Craig')", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.deleteUser('')", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    void testSuccessfulListUsers( S subject, String[] users )
    {
        executeQuery( subject, "CALL dbms.listUsers() YIELD value AS users RETURN users",
                r -> assertKeyIsArray( r, "users", users ) );
    }

    void testFailListUsers( S subject, int count )
    {
        testCallFail( subject,
                "CALL dbms.listUsers() YIELD username",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    void testSuccessfulListRoles( S subject, String[] roles )
    {
        executeQuery( subject, "CALL dbms.listRoles() YIELD value AS roles RETURN roles",
                r -> assertKeyIsArray( r, "roles", roles ) );
    }

    void testFailListRoles( S subject )
    {
        testCallFail( subject,
                "CALL dbms.listRoles() YIELD role",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    void testFailListUserRoles( S subject, String username )
    {
        testCallFail( subject,
                "CALL dbms.listRolesForUser('" + username + "') YIELD value AS roles RETURN count(roles)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    void testFailListRoleUsers( S subject, String roleName )
    {
        testCallFail( subject,
                "CALL dbms.listUsersForRole('" + roleName + "') YIELD value AS users RETURN count(users)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    List<Object> getObjectsAsList( Result r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( Collectors.toList() );
    }

    void assertKeyIs( Result r, String key, String... items )
    {
        assertKeyIsArray( r, key, items );
    }

    void assertKeyIsArray( Result r, String key, String[] items )
    {
        List<Object> results = getObjectsAsList( r, key );
        Assert.assertThat( results, containsInAnyOrder( items ) );
        assertEquals( Arrays.asList( items ).size(), results.size() );
    }

    protected void assertKeyIsMap( Result r, String keyKey, String valueKey, Map<String,Object> expected )
    {
        r.stream().forEach( s -> {
            String key = (String) s.get( keyKey );
            List<String> value = (List<String>) s.get( valueKey );
            assertTrue( "Expected to find values for '" + key + "'", expected.containsKey( key ) );
            List<String> expectedValues = (List<String>) expected.get( key );
            assertEquals(
                    "Results for '" + key + "' should have size " + expectedValues.size() + " but was " + value.size(),
                    value.size(), expectedValues.size() );
            assertThat( value, containsInAnyOrder( expectedValues.toArray() ) );
        } );
    }


    void testCallFail( S subject, String call,
            Class expectedExceptionClass, String partOfErrorMsg )
    {
        try
        {
            testCallEmpty( subject, call, null );
            fail( "Expected exception to be thrown" );
        }
        catch ( Exception e )
        {
            assertEquals( expectedExceptionClass, e.getClass() );
            assertThat( e.getMessage(), containsString( partOfErrorMsg ) );
        }
    }

    void testUnAuthenticated( S subject )
    {
        assertFalse( neo.isAuthenticated( subject ) );
    }

    protected void testUnAuthenticated( S subject, String call )
    {
        //TODO: OMG improve thrown exception
        try
        {
            testCallEmpty( subject, call, null );
            fail( "Allowed un-authenticated query!" );
        }
        catch ( Exception e )
        {
            assertEquals( NullPointerException.class, e.getClass() );
        }
    }

    void testCallEmpty( S subject, String call )
    {
        testCallEmpty( subject, call, null );
    }

    void testCallEmpty( S subject, String call, Map<String,Object> params )
    {
        neo.executeQuery( subject, call, params, ( res ) -> assertFalse( "Expected no results", res.hasNext() ) );
    }

    void testCallCount( S subject, String call, Map<String,Object> params,
            final int count )
    {
        neo.executeQuery( subject, call, params, ( res ) -> {
            int left = count;
            while ( left > 0 )
            {
                assertTrue( "Expected " + count + " results, but got only " + (count - left), res.hasNext() );
                res.next();
                left--;
            }
            assertFalse( "Expected " + count + " results, but there are more ", res.hasNext() );
        } );
    }

    void executeQuery( S subject, String call )
    {
        neo.executeQuery( subject, call, null, r -> {} );
    }

    void executeQuery( S subject, String call, Consumer<Result> resultConsumer )
    {
        neo.executeQuery( subject, call, null, resultConsumer );
    }
}
