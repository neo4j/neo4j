/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher;

import org.junit.Rule;
import org.junit.Test;
import scala.Option;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.planning.CypherCacheHitMonitor;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryInvalidationIT
{
    private static final int USERS = 100;
    private static final int CONNECTIONS = 100;

    @Rule
    public final DbmsRule db = new ImpermanentDbmsRule()
            .withSetting( GraphDatabaseSettings.query_statistics_divergence_threshold, 0.1 )
            .withSetting( GraphDatabaseSettings.cypher_min_replan_interval, Duration.ofSeconds( 1 ) );

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
        assertThat( "Replan should have occurred after TTL", monitor.waitTime.get(), greaterThanOrEqualTo( 1L ) );
    }

    @Test
    public void shouldRePlanAfterDataChangesFromAPopulatedDatabase() throws Exception
    {
        // GIVEN
        Config config = db.getDependencyResolver().resolveDependency( Config.class );
        double divergenceThreshold = config.get( GraphDatabaseSettings.query_statistics_divergence_threshold );
        long replanInterval = config.get( GraphDatabaseSettings.cypher_min_replan_interval ).toMillis();

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
        assertThat( "Replan should have occurred after TTL", monitor.waitTime.get(), greaterThanOrEqualTo( replanInterval / 1000 ) );
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "User" ) ).on( "userId" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, SECONDS );
            tx.commit();
        }
    }

    private void createData( long startingUserId, int numUsers, int numConnections )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( long userId = startingUserId; userId < numUsers + startingUserId; userId++ )
            {
                transaction.execute( "CREATE (newUser:User {userId: $userId})", singletonMap( "userId", userId ) );
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
                transaction.execute( "MATCH (user1:User { userId: $user1 }), (user2:User { userId: $user2 }) " + "MERGE (user1) -[:FRIEND]- (user2)", params );
            }
            transaction.commit();
        }
    }

    private void executeDistantFriendsCountQuery( int userId )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Map<String,Object> params = singletonMap( "userId", (long) randomInt( userId ) );

            try ( Result result = transaction.execute(
                    "MATCH (user:User { userId: $userId } ) -[:FRIEND]- () -[:FRIEND]- (distantFriend) " + "RETURN COUNT(distinct distantFriend)", params ) )
            {
                while ( result.hasNext() )
                {
                    result.next();
                }
            }
            transaction.commit();
        }
    }

    private static int randomInt( int max )
    {
        return ThreadLocalRandom.current().nextInt( max );
    }

    private static class TestMonitor implements CypherCacheHitMonitor<Pair<String,scala.collection.immutable.Map<String, Class<?>>>>
    {
        private final AtomicInteger hits = new AtomicInteger();
        private final AtomicInteger misses = new AtomicInteger();
        private final AtomicInteger discards = new AtomicInteger();
        private final AtomicInteger recompilations = new AtomicInteger();
        private final AtomicLong waitTime = new AtomicLong();

        @Override
        public void cacheHit( Pair<String,scala.collection.immutable.Map<String, Class<?>>> key )
        {
            hits.incrementAndGet();
        }

        @Override
        public void cacheMiss( Pair<String,scala.collection.immutable.Map<String, Class<?>>> key )
        {
            misses.incrementAndGet();
        }

        @Override
        public void cacheDiscard( Pair<String,scala.collection.immutable.Map<String, Class<?>>> key, String ignored, int secondsSinceReplan,
                                  Option<String> maybeReason )
        {
            discards.incrementAndGet();
            waitTime.addAndGet( secondsSinceReplan );
        }

        @Override
        public void cacheRecompile( Pair<String,scala.collection.immutable.Map<String,Class<?>>> key )
        {
            recompilations.incrementAndGet();
        }

        @Override
        public String toString()
        {
            return "TestMonitor{hits=" + hits + ", misses=" + misses + ", discards=" + discards + ", waitTime=" +
                   waitTime + ", recompilations=" + recompilations +  "}";
        }

        public void reset()
        {
            hits.set( 0 );
            recompilations.set( 0 );
            misses.set( 0 );
            discards.set( 0 );
            waitTime.set( 0 );
        }
    }
}
