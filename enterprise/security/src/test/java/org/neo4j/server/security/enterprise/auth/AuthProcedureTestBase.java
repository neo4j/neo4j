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
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
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
    protected EnterpriseAuthSubject adminSubject;
    protected EnterpriseAuthSubject schemaSubject;
    protected EnterpriseAuthSubject writeSubject;
    protected EnterpriseAuthSubject readSubject;
    protected EnterpriseAuthSubject pwdSubject;
    protected EnterpriseAuthSubject noneSubject;

    protected String[] initialUsers = { "adminSubject", "readSubject", "schemaSubject",
        "readWriteSubject", "pwdSubject", "noneSubject", "neo4j" };
    protected String[] initialRoles = { "admin", "architect", "publisher", "reader", "empty" };

    protected GraphDatabaseAPI db;
    protected MultiRealmAuthManager manager;
    protected InternalFlatFileRealm internalRealm;
    protected EnterpriseUserManager userManager;

    @Before
    public void setUp() throws Throwable
    {
        db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
        internalRealm = new InternalFlatFileRealm( new InMemoryUserRepository(), new InMemoryRoleRepository(),
                new BasicPasswordPolicy(), new RateLimitedAuthenticationStrategy( systemUTC(), 3 ) );
        manager = new MultiRealmAuthManager( internalRealm, Collections.singletonList( internalRealm ) );
        manager.init();
        manager.start();
        userManager = manager.getUserManager();
        userManager.newUser( "noneSubject", "abc", false );
        userManager.newUser( "pwdSubject", "abc", true );
        userManager.newUser( "adminSubject", "abc", false );
        userManager.newUser( "schemaSubject", "abc", false );
        userManager.newUser( "readWriteSubject", "abc", false );
        userManager.newUser( "readSubject", "123", false );
        // Currently admin, architect, publisher and reader roles are created by default
        userManager.addUserToRole( "adminSubject", ADMIN );
        userManager.addUserToRole( "schemaSubject", ARCHITECT );
        userManager.addUserToRole( "readWriteSubject", PUBLISHER );
        userManager.addUserToRole( "readSubject", READER );
        userManager.newRole( "empty" );
        noneSubject = manager.login( authToken( "noneSubject", "abc" ) );
        pwdSubject = manager.login( authToken( "pwdSubject", "abc" ) );
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

    protected void testSuccessfulRead( AuthSubject subject, int count )
    {
        testCallCount( subject, "MATCH (n) RETURN n", null, count );
    }

    protected void testFailRead( AuthSubject subject, int count )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "MATCH (n) RETURN n",
                AuthorizationViolationException.class, "Read operations are not allowed" );
    }

    protected void testSuccessfulWrite( AuthSubject subject )
    {
        testCallEmpty( subject, "CREATE (:Node)" );
    }

    protected void testFailWrite( AuthSubject subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "CREATE (:Node)",
                AuthorizationViolationException.class, "Write operations are not allowed" );
    }

    protected void testSuccessfulSchema( AuthSubject subject )
    {
        testCallEmpty( subject, "CREATE INDEX ON :Node(number)" );
    }

    protected void testFailSchema( AuthSubject subject )
    {
        // TODO: this should be permission denied instead
        testCallFail( subject,
                "CREATE INDEX ON :Node(number)",
                AuthorizationViolationException.class, "Schema operations are not allowed" );
    }

    protected void testFailCreateUser( AuthSubject subject )
    {
        testCallFail( subject, "CALL dbms.createUser('Craig', 'foo', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.createUser('Craig', '', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.createUser('', 'foo', false)", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailAddUserToRole( AuthSubject subject )
    {
        testCallFail( subject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailRemoveUserFromRole( AuthSubject subject )
    {
        testCallFail( subject, "CALL dbms.removeUserFromRole('Craig', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailDeleteUser( AuthSubject subject )
    {
        testCallFail( subject, "CALL dbms.deleteUser('Craig')", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
        testCallFail( subject, "CALL dbms.deleteUser('')", QueryExecutionException.class,
                AuthProcedures.PERMISSION_DENIED );
    }

    protected void testSuccessfulListUsers( AuthSubject subject, String[] users )
    {
        testResult( subject, "CALL dbms.listUsers() YIELD username",
                r -> assertKeyIsArray( r, "username", users ) );
    }

    protected void testFailListUsers( AuthSubject subject, int count )
    {
        testCallFail( subject,
                "CALL dbms.listUsers() YIELD username",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testSuccessfulListRoles( AuthSubject subject, String[] roles )
    {
        testResult( subject, "CALL dbms.listRoles() YIELD role",
                r -> assertKeyIsArray( r, "role", roles ) );
    }

    protected void testFailListRoles( AuthSubject subject )
    {
        testCallFail( subject,
                "CALL dbms.listRoles() YIELD role",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailListUserRoles( AuthSubject subject, String username )
    {
        testCallFail( subject,
                "CALL dbms.listRolesForUser('" + username + "') YIELD value AS roles RETURN count(roles)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected void testFailListRoleUsers( AuthSubject subject, String roleName )
    {
        testCallFail( subject,
                "CALL dbms.listUsersForRole('" + roleName + "') YIELD value AS users RETURN count(users)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    protected List<Object> getObjectsAsList( Result r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( Collectors.toList() );
    }

    protected void assertKeyIs( Result r, String key, String... items )
    {
        assertKeyIsArray( r, key, items );
    }

    protected void assertKeyIsArray( Result r, String key, String[] items )
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

    protected void testCall( AuthSubject subject, String call, Consumer<Map<String,Object>> consumer )
    {
        testCall( subject, call, null, consumer );
    }

    protected void testCall( AuthSubject subject, String call, Map<String,Object> params,
            Consumer<Map<String,Object>> consumer )
    {
        testResult( subject, call, params, ( res ) -> {
            if ( res.hasNext() )
            {
                Map<String,Object> row = res.next();
                consumer.accept( row );
            }
            assertFalse( res.hasNext() );
        } );
    }

    protected void testCallFail( AuthSubject subject, String call,
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

    protected void testUnAunthenticated( AuthSubject subject )
    {
        //TODO: improve me to be less gullible!
        assertTrue( subject instanceof EnterpriseAuthSubject );
        assertFalse( ((EnterpriseAuthSubject) subject).getShiroSubject().isAuthenticated() );
    }

    protected void testUnAunthenticated( EnterpriseAuthSubject subject, String call )
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

    protected void testCallEmpty( AuthSubject subject, String call )
    {
        testCallEmpty( subject, call, null );
    }

    protected void testCallEmpty( AuthSubject subject, String call, Map<String,Object> params )
    {
        testResult( subject, call, params, ( res ) -> assertFalse( "Expected no results", res.hasNext() ) );
    }

    protected void testCallCount( AuthSubject subject, String call, Map<String,Object> params,
            final int count )
    {
        testResult( subject, call, params, ( res ) -> {
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

    protected void testResult( AuthSubject subject, String call,
            Consumer<Result> resultConsumer )
    {
        testResult( subject, call, null, resultConsumer );
    }

    protected void testResult( AuthSubject subject, String call, Map<String,Object> params,
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
