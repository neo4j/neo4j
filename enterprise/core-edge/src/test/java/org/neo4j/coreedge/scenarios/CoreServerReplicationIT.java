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

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TargetDirectory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class CoreServerReplicationIT
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;

    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown() throws ExecutionException, InterruptedException
    {
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void shouldReplicateTransactionsToCoreServers() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new SharedDiscoveryService() );

        // when
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );
        CoreGraphDatabase last = cluster.coreTx( ( db, tx ) -> {
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
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new SharedDiscoveryService() );

        cluster.addCoreServerWithServerId( 3, 4 );

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.addCoreServerWithServerId( 4, 5 );
        CoreGraphDatabase last = cluster.coreTx( ( db, tx ) -> {
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
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new SharedDiscoveryService() );

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.removeCoreServer( cluster.awaitLeader() );
        CoreGraphDatabase last = cluster.coreTx( ( db, tx ) -> {
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
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new SharedDiscoveryService() );

        // when
        CoreGraphDatabase last = null;
        for ( int i = 0; i < 15; i++ )
        {
            last = cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }

        cluster.addCoreServerWithServerId( 3, 4 );
        cluster.addCoreServerWithServerId( 4, 5 );

        // then
        assertEquals( 15, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreServers() );
    }

    private long countNodes( CoreGraphDatabase db )
    {
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }

    private void dataMatchesEventually( CoreGraphDatabase sourceDB, Set<CoreGraphDatabase> targetDBs ) throws
            TimeoutException, InterruptedException
    {
        DbRepresentation sourceRepresentation = DbRepresentation.of( sourceDB );
        for ( CoreGraphDatabase targetDB : targetDBs )
        {
            Predicates.await( () -> sourceRepresentation.equals( DbRepresentation.of( targetDB ) ),
                    DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }
}
