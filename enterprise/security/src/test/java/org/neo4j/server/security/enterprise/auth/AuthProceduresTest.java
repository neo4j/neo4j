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
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.time.Clock.systemUTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthProceduresTest
{
    private AuthSubject adminSubject;
    private AuthSubject schemaSubject;
    private AuthSubject writeSubject;
    private AuthSubject readSubject;

    private GraphDatabaseAPI db;
    private ShiroAuthManager manager;

    @Before
    public void setUp() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedures( db );
        manager = new EnterpriseAuthManager( new InMemoryUserRepository(), new InMemoryRoleRepository(),
                new BasicPasswordPolicy(), systemUTC(), true );
        manager.newUser( "admin", "abc", false );
        manager.newUser( "schema", "abc", false );
        manager.newUser( "writer", "abc", false );
        manager.newUser( "reader", "123", false );
        manager.newRole( PredefinedRolesBuilder.ADMIN, "admin" );
        manager.newRole( PredefinedRolesBuilder.ARCHITECT, "architect" );
        manager.newRole( PredefinedRolesBuilder.PUBLISHER, "publisher" );
        manager.newRole( PredefinedRolesBuilder.READER, "reader" );
        readSubject = manager.login( "reader", "123" );
        writeSubject = manager.login( "publisher", "abc" );
        schemaSubject = manager.login( "architect", "abc" );
        adminSubject = manager.login( "admin", "abc" );
        db.execute( "UNWIND range(0,2) AS number CREATE (:Node {number:number})" );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldCreateUser() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('craig', '1234', true)", null );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('craig', '1234', true)", null );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
        try
        {
            testCallEmpty( db, adminSubject, "CALL dbms.createUser('craig', '1234', true)", null );
            fail( "Expected exception to be thrown" );
        }
        catch ( QueryExecutionException e )
        {
            assertTrue( "Exception should contain 'The specified user already exists''",
                    e.getMessage().contains( "The specified user already exists" ) );
        }
    }

    @Test
    public void shouldNotAllowNonAdminCreateUser() throws Exception
    {
        testFailCreateUser( readSubject );
        testFailCreateUser( writeSubject );
        testFailCreateUser( schemaSubject );
    }

    @Test
    public void shouldAllowUserChangePassword() throws Exception
    {
        testCallEmpty( db, readSubject, "CALL dbms.changePassword( '321' )", null );
        AuthSubject subject = manager.login( "reader", "321" );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
    }

    //----------User creation scenarios-----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to group read-only
    Henrik logs in with incorrect password → fail
    Henrik logs in with correct password → gets prompted to change
    Henrik starts read transaction → permission denied
    Henrik changes password to foo → ok
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    public void userCreation1() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)", null );
        manager.addUserToRole( "Henrik", PredefinedRolesBuilder.READER ); // TODO: use procedure when implemented
        AuthSubject subject = manager.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        subject = manager.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testFailRead( subject, 3L );
        testCallEmpty( db, subject, "CALL dbms.changePassword( 'foo' )", null );
        subject = manager.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3L );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password (gets prompted to change - change to foo)
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to group read-only
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    public void userCreation2() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)", null );
        AuthSubject subject = manager.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testCallEmpty( db, subject, "CALL dbms.changePassword( 'foo' )", null );
        subject = manager.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3L );
        manager.addUserToRole( "Henrik", PredefinedRolesBuilder.READER ); // TODO: use procedure when implemented
        testFailWrite( subject );
        testSuccessfulRead( subject, 3L );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to group read-write
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation3() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)", null );
        AuthSubject subject = manager.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3L );
        manager.addUserToRole( "Henrik", PredefinedRolesBuilder.PUBLISHER );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4L );
        testFailSchema( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Henrik starts write transaction → permission denied
    Henrik starts schema transaction → permission denied
    Henrik creates user Craig → permission denied
    Admin adds user Henrik to group Architect
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → ok
    Henrik creates user Craig → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation4() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)", null );
        AuthSubject subject = manager.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3L );
        testFailWrite( subject );
        testFailSchema( subject );
        testFailCreateUser( subject );
        manager.addUserToRole( "Henrik", PredefinedRolesBuilder.ARCHITECT );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4L );
        testSuccessfulSchema( subject );
        testFailCreateUser( subject );
    }

    //-------------Helper functions---------------

    private void testSuccessfulRead( AuthSubject subject, Long count )
    {
        testCall( db, subject, "MATCH (n) RETURN count(n)", ( r ) -> assertEquals( r.get( "count(n)" ), count ) );
    }

    private void testFailRead( AuthSubject subject, Long count )
    {
        try
        {
            testSuccessfulRead( subject, count );
            fail( "Expected exception to be thrown" );
        }
        catch ( AuthorizationViolationException e )
        {
            // TODO: this should be permission denied instead
            assertTrue( "Exception should contain 'Read operations are not allowed'",
                    e.getMessage().contains( "Read operations are not allowed" ) );
        }
    }

    private void testSuccessfulWrite( AuthSubject subject )
    {
        testCallEmpty( db, subject, "CREATE (:Node)", null );
    }

    private void testFailWrite( AuthSubject subject )
    {
        try
        {
            testSuccessfulWrite( subject );
            fail( "Expected exception to be thrown" );
        }
        catch ( AuthorizationViolationException e )
        {
            // TODO: this should be permission denied instead
            assertTrue( "Exception should contain 'Write operations are not allowed'",
                    e.getMessage().contains( "Write operations are not allowed" ) );
        }
    }

    private void testSuccessfulSchema( AuthSubject subject )
    {
        testCallEmpty( db, subject, "CREATE INDEX ON :Node(number)", null );
    }

    private void testFailSchema( AuthSubject subject )
    {
        try
        {
            testSuccessfulSchema( subject );
            fail( "Expected exception to be thrown" );
        }
        catch ( AuthorizationViolationException e )
        {
            // TODO: this should be permission denied instead
            assertTrue( "Exception should contain 'Schema operations are not allowed'",
                    e.getMessage().contains( "Schema operations are not allowed" ) );
        }
    }

    private void testFailCreateUser( AuthSubject subject )
    {
        try
        {
            testCallEmpty( db, subject, "CALL dbms.createUser('Craig', 'foo', false)", null );
            fail( "Expected exception to be thrown" );
        }
        catch ( QueryExecutionException e )
        {
            assertTrue( "Exception should contain '" + AuthProcedures.PERMISSION_DENIED + "'",
                    e.getMessage().contains( AuthProcedures.PERMISSION_DENIED ) );
        }
    }

    private static void testCall( GraphDatabaseAPI db, AuthSubject subject, String call,
            Consumer<Map<String,Object>> consumer )
    {
        testCall( db, subject, call, null, consumer );
    }

    private static void testCall( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
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

    public static void testCallEmpty( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params )
    {
        testResult( db, subject, call, params, ( res ) -> assertFalse( "Expected no results", res.hasNext() ) );
    }

    public static void testCallCount( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
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

    public static void testResult( GraphDatabaseAPI db, AuthSubject subject, String call,
            Consumer<Result> resultConsumer )
    {
        testResult( db, subject, call, null, resultConsumer );
    }

    public static void testResult( GraphDatabaseAPI db, AuthSubject subject, String call, Map<String,Object> params,
            Consumer<Result> resultConsumer )
    {
        try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.explicit, subject ) )
        {
            Map<String,Object> p = (params == null) ? Collections.<String,Object>emptyMap() : params;
            resultConsumer.accept( db.execute( call, p ) );
            tx.success();
        }
    }

    public static void registerProcedures( GraphDatabaseAPI db ) throws KernelException
    {
        Procedures procedures = db.getDependencyResolver().resolveDependency( Procedures.class );
        (new ProceduresProvider()).registerProcedures( procedures );
    }
}
