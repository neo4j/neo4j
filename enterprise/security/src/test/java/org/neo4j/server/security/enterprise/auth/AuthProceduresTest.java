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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.time.Clock.systemUTC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthProceduresTest
{
    private AuthSubject adminSubject;
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
        manager.newUser( "reader", "123", false );
        manager.newRole( PredefinedRolesBuilder.ADMIN, "admin" );
        manager.newRole( PredefinedRolesBuilder.READER, "reader" );
        readSubject = manager.login( "reader", "123" );
        adminSubject = manager.login( "admin", "abc" );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldCreateUser() throws Exception
    {
        testCallEmpty( db, adminSubject, "call dbms.createUser('craig', '1234', true)", null );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {
        testCallEmpty( db, adminSubject, "call dbms.createUser('craig', '1234', true)", null );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
        try
        {
            testCallEmpty( db, adminSubject, "call dbms.createUser('craig', '1234', true)", null );
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
        try
        {
            testCallEmpty( db, readSubject, "call dbms.createUser('craig', '1234', true)", null );
            fail( "Expected exception to be thrown" );
        }
        catch ( AuthorizationViolationException e )
        {
            assertTrue( "Exception should contain 'Permission Denied'",
                    e.getMessage().contains( "Permission Denied" ) );
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
