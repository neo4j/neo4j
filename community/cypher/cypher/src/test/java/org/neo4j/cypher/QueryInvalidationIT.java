/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cypher.internal.compiler.v2_3.CypherCacheHitMonitor;
import org.neo4j.cypher.internal.compiler.v2_3.ast.Query;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.RepeatRule;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class QueryInvalidationIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldRePlanAfterDataChangesFromAnEmptyDatabase() throws Exception
    {
        // GIVEN
        Random random = ThreadLocalRandom.current();
        int USERS = 10, CONNECTIONS = 100;
        TestMonitor monitor = new TestMonitor();
        db.resolveDependency( Monitors.class ).addMonitorListener( monitor );
        // - setup schema -
        db.execute( "CREATE INDEX ON :User(userId)" );
        // - execute the query without the existence data -
        distantFriend( random, USERS );

        long replanTime = System.currentTimeMillis() + 1_800;

        // - create data -
        createData( 0, USERS, CONNECTIONS, random );

        // - after the query TTL has expired -
        while ( System.currentTimeMillis() < replanTime )
        {
            Thread.sleep( 100 );
        }

        // WHEN
        monitor.reset();
        // - execute the query again -
        distantFriend( random, USERS );

        // THEN
        assertEquals( "Query should have been replanned.", 1, monitor.discards.get() );
    }

    @Test
    public void shouldRePlanAfterDataChangesFromAPopulatedDatabase() throws Exception
    {
        // GIVEN
        Random random = ThreadLocalRandom.current();
        int USERS = 10, CONNECTIONS = 100;
        TestMonitor monitor = new TestMonitor();
        db.resolveDependency( Monitors.class ).addMonitorListener( monitor );
        // - setup schema -
        db.execute( "CREATE INDEX ON :User(userId)" );
        //create some data
        createData( 0, USERS, CONNECTIONS, random );
        distantFriend( random, USERS );

        long replanTime = System.currentTimeMillis() + 1_800;

        //create more date
        createData( USERS, USERS, CONNECTIONS, random );

        // - after the query TTL has expired -
        while ( System.currentTimeMillis() < replanTime )
        {
            Thread.sleep( 100 );
        }

        // WHEN
        monitor.reset();
        // - execute the query again -
        distantFriend( random, USERS );

        // THEN
        assertEquals( "Query should have been replanned.", 1, monitor.discards.get() );
    }

    private void createData(long startingUserId, int numUsers, int numConnections, Random random)
    {
        for ( long userId = startingUserId; userId < numUsers + startingUserId; userId++ )
        {
            db.execute( "CREATE (newUser:User {userId: {userId}})", singletonMap( "userId", (Object) userId ) );
        }
        Map<String, Object> params = new HashMap<>();
        for ( int i = 0; i < numConnections; i++ )
        {
            long user1 = startingUserId + random.nextInt( numUsers );
            long user2;
            do
            {
                user2 = startingUserId + random.nextInt( numUsers );
            } while ( user1 == user2 );
            params.put( "user1", user1 );
            params.put( "user2", user2 );
            db.execute( "MATCH (user1:User { userId: {user1} }), (user2:User { userId: {user2} }) " +
                        "CREATE UNIQUE user1 -[:FRIEND]- user2", params );
        }
    }

    private Pair<Long, ExecutionPlanDescription> distantFriend( Random random, int USERS )
    {
        Result result = db
                .execute( "MATCH (user:User { userId: {userId} } ) -[:FRIEND]- () -[:FRIEND]- (distantFriend) " +
                          "RETURN COUNT(distinct distantFriend)",
                          singletonMap( "userId", (Object) (long) random.nextInt( USERS ) ) );
        return Pair.of( (Long) single( single( result ).values() ), result.getExecutionPlanDescription() );
    }

    private static class TestMonitor implements CypherCacheHitMonitor<Query>
    {
        private final AtomicInteger hits = new AtomicInteger();
        private final AtomicInteger misses = new AtomicInteger();
        private final AtomicInteger discards = new AtomicInteger();

        @Override
        public synchronized void cacheHit( Query key )
        {
            hits.incrementAndGet();
        }

        @Override
        public synchronized void cacheMiss( Query key )
        {
            misses.incrementAndGet();
        }

        @Override
        public synchronized void cacheDiscard( Query key )
        {
            discards.incrementAndGet();
        }

        @Override
        public String toString() {
            return "TestMonitor{" +
                    "hits=" + hits +
                    ", misses=" + misses +
                    ", discards=" + discards +
                    '}';
        }

        public void reset()
        {
            hits.set(0);
            misses.set(0);
            discards.set(0);
        }
    }
}
