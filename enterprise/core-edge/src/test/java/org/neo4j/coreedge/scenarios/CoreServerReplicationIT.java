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
package org.neo4j.coreedge.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreServer;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class CoreServerReplicationIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReplicateTransactionsToCoreServers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );
        CoreServer last = cluster.coreTx( ( db, tx ) -> {
            db.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreServers() );
    }

    @Test
    public void shouldReplicateTransactionToCoreServerAddedAfterInitialStartUp() throws Exception
    {
        // given
        cluster.addCoreServerWithServerId( 3, 4 ).start();

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.addCoreServerWithServerId( 4, 5 ).start();
        CoreServer last = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreServers() );
    }

    @Test
    public void shouldReplicateTransactionAfterLeaderWasRemovedFromCluster() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.removeCoreServer( cluster.awaitLeader() );
        CoreServer last = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreServers() );
    }

    @Test
    public void shouldReplicateToCoreServersAddedAfterInitialTransactions() throws Exception
    {
        // when
        CoreServer last = null;
        for ( int i = 0; i < 15; i++ )
        {
            last = cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }

        cluster.addCoreServerWithServerId( 3, 4 ).start();
        cluster.addCoreServerWithServerId( 4, 5 ).start();

        // then
        assertEquals( 15, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreServers() );
    }

    @Test
    public void shouldReplicateTransactionsToReplacementCoreServers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.removeCoreServerWithServerId( 0 );
        CoreServer replacement = cluster.addCoreServerWithServerId( 0, 3 );
        replacement.start();

        CoreServer leader = cluster.coreTx( ( db, tx ) -> {
            db.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreServers() );
    }

    private long countNodes( CoreServer server )
    {
        CoreGraphDatabase db = server.database();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }
}
