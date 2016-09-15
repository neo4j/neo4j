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

import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.coreedge.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class CoreReplicationIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReplicateTransactionsToCoreMembers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );
        CoreClusterMember last = cluster.coreTx( ( db, tx ) -> {
            db.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateTransactionToCoreMemberAddedAfterInitialStartUp() throws Exception
    {
        // given
        cluster.addCoreMemberWithId( 3 ).start();

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.addCoreMemberWithId( 4 ).start();
        CoreClusterMember last = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
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
        cluster.removeCoreMember( cluster.awaitLeader() );
        CoreClusterMember last = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateToCoreMembersAddedAfterInitialTransactions() throws Exception
    {
        // when
        CoreClusterMember last = null;
        for ( int i = 0; i < 15; i++ )
        {
            last = cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }

        cluster.addCoreMemberWithId( 3 ).start();
        cluster.addCoreMemberWithId( 4 ).start();

        // then
        assertEquals( 15, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateTransactionsToReplacementCoreMembers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.removeCoreMemberWithMemberId( 0 );
        CoreClusterMember replacement = cluster.addCoreMemberWithId( 0 );
        replacement.start();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) -> {
            db.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }

    private long countNodes( CoreClusterMember member )
    {
        CoreGraphDatabase db = member.database();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }
}
