/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseSelectionStrategy;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.neo4j.causalclustering.scenarios.ReadReplicaToReadReplicaCatchupIT.SpecificReplicaStrategy.upstreamFactory;
import static org.neo4j.com.storecopy.StoreUtil.TEMP_COPY_DIRECTORY_NAME;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaToReadReplicaCatchupIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                    .withDiscoveryServiceFactory( new HazelcastDiscoveryServiceFactory() );

    @Test
    public void shouldEventuallyPullTransactionAcrossReadReplicas() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodesToCreate = 100;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        storeSomeDataInCores( cluster, numberOfNodesToCreate );

        AtomicBoolean labelScanStoreCorrectlyPlaced = new AtomicBoolean( false );
        Monitors monitors = new Monitors();
        ReadReplica firstReadReplica = cluster.addReadReplicaWithIdAndMonitors( 101, monitors );

        File labelScanStore = LabelScanStoreProvider
                .getStoreDirectory( new File( firstReadReplica.storeDir(), TEMP_COPY_DIRECTORY_NAME ) );

        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            if ( file.getParent().contains( labelScanStore.getPath() ) )
            {
                labelScanStoreCorrectlyPlaced.set( true );
            }
        } );

        firstReadReplica.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.stopCatchupServer();
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
            db.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        storeSomeDataInCores( cluster, numberOfNodes );

        AtomicBoolean labelScanStoreCorrectlyPlaced = new AtomicBoolean( false );
        Monitors monitors = new Monitors();

        ReadReplica firstReadReplica =
                cluster.addReadReplicaWithIdAndMonitors( firstReadReplicaLocalMemberId, monitors );
        File labelScanStore = LabelScanStoreProvider
                .getStoreDirectory( new File( firstReadReplica.storeDir(), TEMP_COPY_DIRECTORY_NAME ) );

        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            if ( file.getParent().contains( labelScanStore.getPath() ) )
            {
                labelScanStoreCorrectlyPlaced.set( true );
            }
        } );

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
        storeSomeDataInCores( cluster, numberOfNodes );

        // then
        // reached second read replica from cores
        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodes * 2 );
    }

    private void storeSomeDataInCores( Cluster cluster, int numberOfNodes ) throws Exception
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                createData( db );
                tx.success();
            } );
        }
    }

    private void createData( GraphDatabaseService db )
    {
        Node node = db.createNode( Label.label( "Foo" ) );
        node.setProperty( "foobar", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar1", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar2", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar3", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar4", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar5", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar6", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar7", format( "baz_bat%s", UUID.randomUUID() ) );
        node.setProperty( "foobar8", format( "baz_bat%s", UUID.randomUUID() ) );
    }

    private void checkDataHasReplicatedToReadReplicas( Cluster cluster, long numberOfNodes ) throws Exception
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
        public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
        {
            ReadReplica current = upstreamFactory.current();
            if ( current == null )
            {
                return Optional.empty();
            }
            else
            {
                return current.memberId();
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
