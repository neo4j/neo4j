/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.neo4j.causalclustering.helpers.DataCreator.createLabelledNodesWithProperty;
import static org.neo4j.causalclustering.scenarios.ReadReplicaToReadReplicaCatchupIT.SpecificReplicaStrategy.upstreamFactory;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaToReadReplicaCatchupIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" )
                    .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, "true" )
                    .withDiscoveryServiceType( DiscoveryServiceType.HAZELCAST );

    @Test
    public void shouldEventuallyPullTransactionAcrossReadReplicas() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodesToCreate = 100;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        createLabelledNodesWithProperty( cluster, numberOfNodesToCreate, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica = cluster.addReadReplicaWithIdAndMonitors( 101, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.disableCatchupServer();
        }

        // when
        upstreamFactory.setCurrent( firstReadReplica );
        ReadReplica secondReadReplica = cluster.addReadReplicaWithId( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        // then

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );
    }

    @Test
    public void shouldCatchUpFromCoresWhenPreferredReadReplicasAreUnavailable() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodes = 1;
        int firstReadReplicaLocalMemberId = 101;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        ReadReplica firstReadReplica =
                cluster.addReadReplicaWithIdAndMonitors( firstReadReplicaLocalMemberId, new Monitors() );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        upstreamFactory.setCurrent( firstReadReplica );

        ReadReplica secondReadReplica = cluster.addReadReplicaWithId( 202 );
        secondReadReplica.setUpstreamDatabaseSelectionStrategy( "specific" );

        secondReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes );

        firstReadReplica.shutdown();
        upstreamFactory.reset();

        cluster.removeReadReplicaWithMemberId( firstReadReplicaLocalMemberId );

        // when
        // More transactions into core
        createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        // reached second read replica from cores
        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes * 2 );
    }

    static void checkDataHasReplicatedToReadReplicas( Cluster cluster, long numberOfNodes ) throws Exception
    {
        for ( final ReadReplica server : cluster.readReplicas() )
        {
            GraphDatabaseService readReplica = server.database();
            try ( Transaction tx = readReplica.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( readReplica.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, is( numberOfNodes ), 1, MINUTES );

                for ( Node node : readReplica.getAllNodes() )
                {
                    assertThat( node.getProperty( "foobar" ).toString(), startsWith( "baz_bat" ) );
                }

                tx.success();
            }
        }
    }

    @Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
    public static class SpecificReplicaStrategy extends UpstreamDatabaseSelectionStrategy
    {
        // This because we need a stable point for config to inject into Service loader loaded classes
        static final UpstreamFactory upstreamFactory = new UpstreamFactory();

        public SpecificReplicaStrategy()
        {
            super( "specific" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            ReadReplica current = upstreamFactory.current();
            if ( current == null )
            {
                return Optional.empty();
            }
            else
            {
                return Optional.of( current.memberId() );
            }
        }
    }

    private static class UpstreamFactory
    {
        private ReadReplica current;

        public void setCurrent( ReadReplica readReplica )
        {
            this.current = readReplica;
        }

        public ReadReplica current()
        {
            return current;
        }

        void reset()
        {
            current = null;
        }
    }
}
