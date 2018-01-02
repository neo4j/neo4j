/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cypher.internal.compiler.v2_3.CypherCacheHitMonitor;
import org.neo4j.cypher.internal.frontend.v2_3.ast.Query;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryInvalidationIT
{
    private static final int USERS = 10;
    private static final int CONNECTIONS = 100;

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( GraphDatabaseSettings.query_statistics_divergence_threshold, "0.5" );
            builder.setConfig( GraphDatabaseSettings.cypher_min_replan_interval, "1s" );
        }
    };

    @Test
    public void shouldRePlanAfterDataChangesFromAnEmptyDatabase() throws Exception
    {
        // GIVEN
        TestMonitor monitor = new TestMonitor();
        db.resolveDependency( Monitors.class ).addMonitorListener( monitor );
        // - setup schema -
        createIndex();
        // - execute the query without the existence data -
        executeDistantFriendsCountQuery( USERS );

        long replanTime = System.currentTimeMillis() + 1_800;

        // - create data -
        createData( 0, USERS, CONNECTIONS );

        // - after the query TTL has expired -
        while ( System.currentTimeMillis() < replanTime )
        {
            Thread.sleep( 100 );
        }

        // WHEN
        monitor.reset();
        // - execute the query again -
        executeDistantFriendsCountQuery( USERS );

        // THEN
        assertEquals( "Query should have been replanned.", 1, monitor.discards.get() );
    }

    @Test
    public void shouldRePlanAfterDataChangesFromAPopulatedDatabase() throws Exception
    {
        // GIVEN
        Config config = db.getConfigCopy();
        double divergenceThreshold = config.get( GraphDatabaseSettings.query_statistics_divergence_threshold );
        long replanInterval = config.get( GraphDatabaseSettings.cypher_min_replan_interval );

        TestMonitor monitor = new TestMonitor();
        db.resolveDependency( Monitors.class ).addMonitorListener( monitor );
        // - setup schema -
        createIndex();
        //create some data
        createData( 0, USERS, CONNECTIONS );
        executeDistantFriendsCountQuery( USERS );

        long replanTime = System.currentTimeMillis() + replanInterval;

        assertTrue( "Test does not work with edge setting for query_statistics_divergence_threshold: " + divergenceThreshold,
                divergenceThreshold > 0.0 && divergenceThreshold < 1.0 );

        int usersToCreate = ((int) (Math.ceil( ((double) USERS) / (1.0 - divergenceThreshold) ))) - USERS + 1;

        //create more data
        createData( USERS, usersToCreate, CONNECTIONS );

        // - after the query TTL has expired -
        while ( System.currentTimeMillis() <= replanTime )
        {
            Thread.sleep( 100 );
        }

        // WHEN
        monitor.reset();
        // - execute the query again -
        executeDistantFriendsCountQuery( USERS );

        // THEN
        assertEquals( "Query should have been replanned.", 1, monitor.discards.get() );
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( DynamicLabel.label( "User" ) ).on( "userId" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    private void createData( long startingUserId, int numUsers, int numConnections )
    {
        for ( long userId = startingUserId; userId < numUsers + startingUserId; userId++ )
        {
            db.execute( "CREATE (newUser:User {userId: {userId}})", singletonMap( "userId", (Object) userId ) );
        }
        Map<String,Object> params = new HashMap<>();
        for ( int i = 0; i < numConnections; i++ )
        {
            long user1 = startingUserId + randomInt( numUsers );
            long user2;
            do
            {
                user2 = startingUserId + randomInt( numUsers );
            }
            while ( user1 == user2 );
            params.put( "user1", user1 );
            params.put( "user2", user2 );
            db.execute( "MATCH (user1:User { userId: {user1} }), (user2:User { userId: {user2} }) " +
                        "CREATE UNIQUE user1 -[:FRIEND]- user2", params );
        }
    }

    private void executeDistantFriendsCountQuery( int userId )
    {
        Map<String,Object> params = singletonMap( "userId", (Object) (long) randomInt( userId ) );

        try ( Result result = db.execute(
                "MATCH (user:User { userId: {userId} } ) -[:FRIEND]- () -[:FRIEND]- (distantFriend) " +
                "RETURN COUNT(distinct distantFriend)", params ) )
        {
            while ( result.hasNext() )
            {
                result.next();
            }
        }
    }

    private static int randomInt( int max )
    {
        return ThreadLocalRandom.current().nextInt( max );
    }

    private static class TestMonitor implements CypherCacheHitMonitor<Query>
    {
        private final AtomicInteger hits = new AtomicInteger();
        private final AtomicInteger misses = new AtomicInteger();
        private final AtomicInteger discards = new AtomicInteger();

        @Override
        public void cacheHit( Query key )
        {
            hits.incrementAndGet();
        }

        @Override
        public void cacheMiss( Query key )
        {
            misses.incrementAndGet();
        }

        @Override
        public void cacheDiscard( Query key )
        {
            discards.incrementAndGet();
        }

        @Override
        public String toString()
        {
            return "TestMonitor{hits=" + hits + ", misses=" + misses + ", discards=" + discards + "}";
        }

        public void reset()
        {
            hits.set( 0 );
            misses.set( 0 );
            discards.set( 0 );
        }
    }
}
