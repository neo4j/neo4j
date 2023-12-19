/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.entireClusterSeesMemberAsNotAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesMembers;

public class BoltHAIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withBoltEnabled().withCluster( clusterOfSize( 3 ) );

    @Test
    public void shouldContinueServingBoltRequestsBetweenInternalRestarts() throws Throwable
    {
        // given
        /*
         * Interestingly, it is enough to simply start a slave and then direct sessions to it. The problem seems
         * to arise immediately, since simply from startup to being into SLAVE at least one internal restart happens
         * and that seems sufficient to break the bolt server.
         * However, that would make the test really weird, so we'll start the cluster, make sure we can connect and
         * then isolate the slave, make it shutdown internally, then have it rejoin and it will switch to slave.
         * At the end of this process, it must still be possible to open and execute transactions against the instance.
         */
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();

        Driver driver = GraphDatabase.driver( cluster.getBoltAddress( slave1 ),
                AuthTokens.basic( "neo4j", "neo4j" ) );

        /*
         * We'll use a bookmark to enforce use of kernel internals by the bolt server, to make sure that parts that are
         * switched during an internal restart are actually refreshed. Technically, this is not necessary, since the
         * bolt server makes such use for every request. But this puts a nice bow on top of it.
         */
        String lastBookmark = inExpirableSession( driver, Driver::session, s ->
        {
            try ( Transaction tx = s.beginTransaction() )
            {
                tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                        parameters( "name", "Webber", "title", "Mr" ) );
                tx.success();
            }
            return s.lastBookmark();
        } );

        // when
        ClusterManager.RepairKit slaveFailRK = cluster.fail( slave1 );

        cluster.await( entireClusterSeesMemberAsNotAvailable( slave1 ) );
        slaveFailRK.repair();
        cluster.await( masterSeesMembers( 3 ) );

        // then
        Integer count = inExpirableSession( driver, Driver::session, s ->
        {
            Record record;
            try ( Transaction tx = s.beginTransaction( lastBookmark ) )
            {
                record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx.success();
            }
            return record.get( "count" ).asInt();
        } );
        assertEquals( 1, count.intValue() );
    }

    private static <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException
    {
        long endTime = System.currentTimeMillis() + 15_000;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( TransientException | SessionExpiredException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }
}
