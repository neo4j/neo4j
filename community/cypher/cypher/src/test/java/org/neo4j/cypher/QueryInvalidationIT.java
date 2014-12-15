/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cypher.internal.compiler.v2_2.CypherCacheHitMonitor;
import org.neo4j.cypher.internal.compiler.v2_2.PreparedQuery;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Collections.singletonMap;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.single;

public class QueryInvalidationIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldRePlanAfterDataChanges() throws Exception
    {
        // GIVEN
        Random random = ThreadLocalRandom.current();
        int USERS = 1000, CONNECTIONS = 10000;
        Monitor monitor = new Monitor();
        db.resolveDependency( Monitors.class ).addMonitorListener( monitor );
        // - setup schema -
        db.execute( "CREATE INDEX ON :User(userId)" );
        // - execute the query without the existence data -
        distantFriend( random, USERS );

        long replanTime = System.currentTimeMillis() + 1_100;

        // - create data -
        for ( long userId = 0; userId < USERS; userId++ )
        {
            db.execute( "CREATE (newUser:User {userId: {userId}})", singletonMap( "userId", (Object) userId ) );
        }
        Map<String, Object> params = new HashMap<>();
        for ( int i = 0; i < CONNECTIONS; i++ )
        {
            long user1 = random.nextInt( USERS );
            long user2;
            do
            {
                user2 = random.nextInt( USERS );
            } while ( user1 == user2 );
            params.put( "user1", user1 );
            params.put( "user2", user2 );
            db.execute( "MATCH (user1:User { userId: {user1} }), (user2:User { userId: {user2} }) " +
                        "CREATE UNIQUE user1 -[:FRIEND]- user2", params );
        }

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
        assertEquals( "Query should have been replanned.", 1, monitor.discards );
    }

    private Pair<Long, ExecutionPlanDescription> distantFriend( Random random, int USERS )
    {
        Result result = db
                .execute( "MATCH (user:User { userId: {userId} } ) -[:FRIEND]- () -[:FRIEND]- (distantFriend) " +
                          "RETURN COUNT(distinct distantFriend)",
                          singletonMap( "userId", (Object) (long) random.nextInt( USERS ) ) );
        return Pair.of( (Long) single( single( result ).values() ), result.getExecutionPlanDescription() );
    }

    private static class Monitor implements CypherCacheHitMonitor<PreparedQuery>
    {
        int hits, misses, discards;

        @Override
        public synchronized void cacheHit( PreparedQuery key )
        {
            hits++;
        }

        @Override
        public synchronized void cacheMiss( PreparedQuery key )
        {
            misses++;
        }

        @Override
        public synchronized void cacheDiscard( PreparedQuery key )
        {
            discards++;
        }

        public void reset()
        {
            hits = misses = discards = 0;
        }
    }
}
