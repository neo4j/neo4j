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
package org.neo4j.causalclustering.discovery;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.NullLogProvider;

import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofMillis;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_advertised_address;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SharedDiscoveryServiceIT
{
    private static final long TIMEOUT_MS = 15_000;
    private static final long RUN_TIME_MS = 1000;

    private NullLogProvider logProvider = getInstance();
    private NullLogProvider userLogProvider = getInstance();

    @Test
    public void shouldDiscoverCompleteTargetSetWithoutDeadlocks()
    {
        assertTimeout( ofMillis( TIMEOUT_MS ), () -> {
            //  given

            ExecutorService es = newCachedThreadPool();

            long endTimeMillis = currentTimeMillis() + RUN_TIME_MS;
            while ( endTimeMillis > currentTimeMillis() )
            {
                Set<MemberId> members = new HashSet<>();
                for ( int i = 0; i < 3; i++ )
                {
                    members.add( new MemberId( randomUUID() ) );
                }

                SharedDiscoveryService sharedService = new SharedDiscoveryService();

                List<Callable<Void>> discoveryJobs = new ArrayList<>();
                for ( MemberId member : members )
                {
                    discoveryJobs.add( createDiscoveryJob( member, sharedService, members ) );
                }

                List<Future<Void>> results = es.invokeAll( discoveryJobs );
                for ( Future<Void> result : results )
                {
                    result.get( TIMEOUT_MS, MILLISECONDS );
                }
            }
            ;
        } );
    }

    private Callable<Void> createDiscoveryJob( MemberId member, DiscoveryServiceFactory disoveryServiceFactory,
            Set<MemberId> expectedTargetSet )
    {
        Neo4jJobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();
        HostnameResolver hostnameResolver = new NoOpHostnameResolver();

        CoreTopologyService topologyService = disoveryServiceFactory
                .coreTopologyService( config(), member, jobScheduler, logProvider, userLogProvider, hostnameResolver,
                        new TopologyServiceNoRetriesStrategy() );
        return sharedClientStarter( topologyService, expectedTargetSet );
    }

    private Config config()
    {
        return defaults( stringMap( raft_advertised_address.name(), "127.0.0.1:7000",
                transaction_advertised_address.name(), "127.0.0.1:7001",
                new BoltConnector( "bolt" ).enabled.name(), "true",
                new BoltConnector( "bolt" ).advertised_address.name(), "127.0.0.1:7002" ) );
    }

    private Callable<Void> sharedClientStarter( CoreTopologyService topologyService, Set<MemberId> expectedTargetSet )
    {
        return () -> {
            try
            {
                RaftMachine raftMock = mock( RaftMachine.class );
                topologyService.start();
                topologyService.addCoreTopologyListener( new RaftCoreTopologyConnector( topologyService, raftMock ) );

                assertEventually( "should discover complete target set", () -> {
                    ArgumentCaptor<Set<MemberId>> targetMembers =
                            forClass( (Class<Set<MemberId>>) expectedTargetSet.getClass() );
                    verify( raftMock, atLeastOnce() ).setTargetMembershipSet( targetMembers.capture() );
                    return targetMembers.getValue();
                }, equalTo( expectedTargetSet ), TIMEOUT_MS, MILLISECONDS );
            }
            catch ( Throwable throwable )
            {
                fail( throwable.getMessage() );
            }
            return null;
        };
    }
}
