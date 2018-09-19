/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.TopologyServiceNoRetriesStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public abstract class BaseCoreTopologyServiceIT
{
    private final DiscoveryServiceType discoveryServiceType;

    protected BaseCoreTopologyServiceIT( DiscoveryServiceType discoveryServiceType )
    {
        this.discoveryServiceType = discoveryServiceType;
    }

    @Test( timeout = 120_000 )
    public void shouldBeAbleToStartAndStopWithoutSuccessfulJoin() throws Throwable
    {
        // Random members that does not exists, discovery will never succeed
        String initialHosts = "localhost:" + PortAuthority.allocatePort() + ",localhost:" + PortAuthority.allocatePort();
        Config config = Config.defaults();
        config.augment( initial_discovery_members, initialHosts );
        config.augment( CausalClusteringSettings.discovery_listen_address, "localhost:" + PortAuthority.allocatePort() );

        JobScheduler jobScheduler = createInitialisedScheduler();
        InitialDiscoveryMembersResolver
                initialDiscoveryMemberResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );

        CoreTopologyService service = discoveryServiceType.createFactory().coreTopologyService(
                config,
                new MemberId( UUID.randomUUID() ),
                jobScheduler,
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                initialDiscoveryMemberResolver,
                new TopologyServiceNoRetriesStrategy(),
                new Monitors() );
        service.init();
        service.start();
        service.stop();
        service.shutdown();
    }

}
