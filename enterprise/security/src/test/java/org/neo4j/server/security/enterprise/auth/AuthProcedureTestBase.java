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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static java.time.Clock.systemUTC;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

public class AuthProcedureTestBase
{
    protected AuthSubject adminSubject;
    protected AuthSubject schemaSubject;
    protected AuthSubject writeSubject;
    protected AuthSubject readSubject;
    protected AuthSubject noneSubject;

    protected String[] initialUsers = { "adminSubject", "readSubject", "schemaSubject",
        "readWriteSubject", "noneSubject", "neo4j" };
    protected String[] initialRoles = { "admin", "architect", "publisher", "reader", "empty" };

    protected GraphDatabaseAPI db;
    protected ShiroAuthManager manager;

    @Before
    public void setUp() throws Throwable
    {
        db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
        manager = new EnterpriseAuthManager( new InMemoryUserRepository(), new InMemoryRoleRepository(),
                new BasicPasswordPolicy(), systemUTC(), true );
        manager.init();
        manager.start();
        manager.newUser( "noneSubject", "abc", false );
        manager.newUser( "adminSubject", "abc", false );
        manager.newUser( "schemaSubject", "abc", false );
        manager.newUser( "readWriteSubject", "abc", false );
        manager.newUser( "readSubject", "123", false );
        // Currently admin role is created by default
        manager.addUserToRole( "adminSubject", ADMIN );
        manager.newRole( ARCHITECT, "schemaSubject" );
        manager.newRole( PUBLISHER, "readWriteSubject" );
        manager.newRole( READER, "readSubject" );
        manager.newRole( "empty" );
        noneSubject = manager.login( authToken( "noneSubject", "abc" ) );
        readSubject = manager.login( authToken( "readSubject", "123" ) );
        writeSubject = manager.login( authToken( "readWriteSubject", "abc" ) );
        schemaSubject = manager.login( authToken( "schemaSubject", "abc" ) );
        adminSubject = manager.login( authToken( "adminSubject", "abc" ) );
        db.execute( "UNWIND range(0,2) AS number CREATE (:Node {number:number})" );
    }

    @After
    public void tearDown() throws Throwable
    {
        db.shutdown();
        manager.stop();
        manager.shutdown();
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

    protected void testSuccessfulReadAction( AuthSubject subject, int count )
    {
        testCallCount( db, subject, "MATCH (n) RETURN n", null, count );
    }

    protected void testFailReadAction( AuthSubject subject, int count )
    {
        // TODO: this should be permission denied instead
        testCallFail( db, subject,
                "MATCH (n) RETURN n",
                AuthorizationViolationException.class, "Read operations are not allowed" );
    }

    protected void testSuccessfulWriteAction( AuthSubject subject )
    {
        testCallEmpty( db, subject, "CREATE (:Node)" );
    }

    protected void testFailWriteAction( AuthSubject subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( db, subject,
                "CREATE (:Node)",
                AuthorizationViolationException.class, "Write operations are not allowed" );
    }

    protected void testSuccessfulSchemaAction( AuthSubject subject )
    {
        testCallEmpty( db, subject, "CREATE INDEX ON :Node(number)" );
    }

    protected void testFailSchema( AuthSubject subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( db, subject,
                "CREATE INDEX ON :Node(number)",
                AuthorizationViolationException.class, "Schema operations are not allowed" );
    }

    protected void testFailCreateUser( AuthSubject subject )
    {
        testCallFail( db, subject, "CALL dbms.createUser('Craig', 'foo', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailAddUserToRoleAction( AuthSubject subject )
    {
        testCallFail( db, subject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailRemoveUserFromRoleAction( AuthSubject subject )
    {
        testCallFail( db, subject, "CALL dbms.removeUserFromRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailDeleteUser( AuthSubject subject )
    {
        testCallFail( db, subject, "CALL dbms.deleteUser('Craig')", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    protected void testSuccessfulListUsersAction( AuthSubject subject, String[] users )
    {
        testResult( db, subject, "CALL dbms.listUsers() YIELD username AS users RETURN users",
                r -> resultKeyIsArray( r, "users", users ) );
    }

    protected void testFailListUsers( AuthSubject subject, int count )
    {
        testCallFail( db, subject,
                "CALL dbms.listUsers() YIELD value AS users RETURN users",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testSuccessfulListRolesAction( AuthSubject subject, String[] roles )
    {
        testResult( db, subject, "CALL dbms.listRoles() YIELD role AS roles RETURN roles",
                r -> resultKeyIsArray( r, "roles", roles ) );
    }

    protected void testFailListRoles( AuthSubject subject )
    {
        testCallFail( db, subject,
                "CALL dbms.listRoles() YIELD value AS roles RETURN roles",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailListUserRoles( AuthSubject subject, String username )
    {
        testCallFail( db, subject,
                "CALL dbms.listRolesForUser('" + username + "') YIELD value AS roles RETURN count(roles)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailListRoleUsers( AuthSubject subject, String roleName )
    {
        testCallFail( db, subject,
                "CALL dbms.listUsersForRole('" + roleName + "') YIELD value AS users RETURN count(users)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected List<Object> getObjectsAsList( Result r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( Collectors.toList() );
    }

    protected void resultKeyIs( Result r, String key, String... items )
    {
        resultKeyIsArray( r, key, items );
    }

    protected void resultKeyIsArray( Result r, String key, String[] items )
        {
        List<Object> results = getObjectsAsList( r, key );
        Assert.assertThat( results, containsInAnyOrder( items ) );
        assertEquals( Arrays.asList( items ).size(), results.size() );
    }

    protected void resultContainsMap( Result r, String keyKey, String valueKey, Map<String,Object> expected )
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

    protected static void testCall( GraphDatabaseAPI db, AuthSubject subject, String call,
            Consumer<Map<String,Object>> consumer )
    {
        testCall( db, subject, call, null, consumer );
    }

    protected static void testCall( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
            Consumer<Map<String,Object>> consumer )
    {
        testResult( db, subject, call, params, ( res ) -> {
            if ( res.hasNext() )
            {
                Map<String,Object> row = res.next();
                consumer.accept( row );
            }
            assertFalse( res.hasNext() );
        } );
    }

    protected static void testCallFail( GraphDatabaseAPI db, AuthSubject subject, String call,
            Class expectedExceptionClass, String partOfErrorMsg )
    {
        try
        {
            testCallEmpty( db, subject, call, null );
            fail( "Expected exception to be thrown" );
        }
        catch ( Exception e )
        {
            assertEquals( expectedExceptionClass, e.getClass() );
            assertThat( e.getMessage(), containsString( partOfErrorMsg ) );
        }
    }

    protected static void testCallEmpty( GraphDatabaseAPI db, AuthSubject subject, String call )
    {
        testCallEmpty( db, subject, call, null );
    }

    protected static void testCallEmpty( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params )
    {
        testResult( db, subject, call, params, ( res ) -> assertFalse( "Expected no results", res.hasNext() ) );
    }

    protected static void testCallCount( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
            final int count )
    {
        testResult( db, subject, call, params, ( res ) -> {
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

    protected static void testResult( GraphDatabaseAPI db, AuthSubject subject, String call,
            Consumer<Result> resultConsumer )
    {
        testResult( db, subject, call, null, resultConsumer );
    }

    protected static void testResult( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
            Consumer<Result> resultConsumer )
    {
        try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.explicit, subject ) )
        {
            Map<String,Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept( db.execute( call, p ) );
            tx.success();
        }
    }
}
