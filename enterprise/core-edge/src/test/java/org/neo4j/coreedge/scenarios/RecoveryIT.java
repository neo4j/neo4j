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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public class RecoveryIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 );

    @Test
    public void shouldBeConsistentAfterShutdown() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreServers().stream()
                .map( CoreGraphDatabase::getStoreDir ).map( File::new ).collect( toSet() );

        // when
        cluster.shutdown();

        storeDirs.forEach( this::assertConsistent );
        assertEquals( 1, storeDirs.stream().map( DbRepresentation::of ).collect( toSet() ).size() );
    }

    @Test
    public void singleServerWithinClusterShouldBeConsistentAfterRestart() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int clusterSize = cluster.numberOfCoreServers();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreServers().stream()
                .map( CoreGraphDatabase::getStoreDir ).map( File::new ).collect( toSet() );

        // when
        for ( int i = 0; i < clusterSize; i++ )
        {
            cluster.removeCoreServerWithServerId( i );
            fireSomeLoadAtTheCluster( cluster );
            cluster.addCoreServerWithServerId( i, clusterSize );
        }

        cluster.shutdown();

        storeDirs.forEach( this::assertConsistent );
        assertEquals( 1, storeDirs.stream().map( DbRepresentation::of ).collect( toSet() ).size() );
    }

    private void assertConsistent( File storeDir )
    {
        ConsistencyCheckService.Result result;
        try
        {
            result = new ConsistencyCheckService().runFullConsistencyCheck( storeDir, Config.defaults(),
                    ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        assertTrue( result.isSuccessful() );
    }

    private void fireSomeLoadAtTheCluster( Cluster cluster ) throws Exception
    {
        for ( int i = 0; i < cluster.numberOfCoreServers(); i++ )
        {
            final String prop = "val" + i;
            cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode( label( "demo" ) );
                node.setProperty( "server", prop );
                tx.success();
            } );
        }
    }
}
