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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RecoveryIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    @Test
    public void shouldBeConsistentAfterShutdown() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreMembers().stream().map( CoreClusterMember::storeDir ).collect( toSet() );

        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( this::dbRepresentation ).collect( toSet() ).size(),
                equalTo( 1 ), 10, TimeUnit.SECONDS );

        // when
        cluster.shutdown();

        // then
        storeDirs.forEach( this::assertConsistent );
    }

    @Test
    public void singleServerWithinClusterShouldBeConsistentAfterRestart() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int clusterSize = cluster.numberOfCoreMembersReportedByTopology();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreMembers().stream().map( CoreClusterMember::storeDir ).collect( toSet() );

        // when
        for ( int i = 0; i < clusterSize; i++ )
        {
            cluster.removeCoreMemberWithServerId( i );
            fireSomeLoadAtTheCluster( cluster );
            cluster.addCoreMemberWithId( i ).start();
        }

        // then
        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( this::dbRepresentation ).collect( toSet() ).size(),
                equalTo( 1 ), 10, TimeUnit.SECONDS );

        cluster.shutdown();

        storeDirs.forEach( this::assertConsistent );
    }

    private DbRepresentation dbRepresentation( CoreClusterMember member )
    {
        return  DbRepresentation.of( member.database() );
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
        for ( int i = 0; i < cluster.numberOfCoreMembersReportedByTopology(); i++ )
        {
            final String prop = "val" + i;
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label( "demo" ) );
                node.setProperty( "server", prop );
                tx.success();
            } );
        }
    }
}
