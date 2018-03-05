/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
