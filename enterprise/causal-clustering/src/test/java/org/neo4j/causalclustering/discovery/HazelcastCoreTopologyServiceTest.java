package org.neo4j.causalclustering.discovery;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class HazelcastCoreTopologyServiceTest
{
    @Test( timeout = 120_000)
    public void shouldBeAbleToStartAndStoreWithoutSuccessfulJoin()
    {
        CentralJobScheduler jobScheduler = new CentralJobScheduler();
        jobScheduler.init();
        HostnameResolver hostnameResolver = new NoOpHostnameResolver();

        // Random members that does not exists, discovery will never succeed
        String initialHosts = "localhost:" + PortAuthority.allocatePort() + ",localhost:" + PortAuthority.allocatePort();
        Config config = config();
        config.augment( initial_discovery_members, initialHosts );
        HazelcastCoreTopologyService service = new HazelcastCoreTopologyService( config,
                new MemberId( UUID.randomUUID() ),
                jobScheduler,
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                hostnameResolver,
                new TopologyServiceNoRetriesStrategy() );
        service.start();
        Thread.yield();
        service.stop();
    }

    private static Config config()
    {
        return Config.defaults( stringMap(
                CausalClusteringSettings.raft_advertised_address.name(), "127.0.0.1:7000",
                CausalClusteringSettings.transaction_advertised_address.name(), "127.0.0.1:7001",
                new BoltConnector( "bolt" ).enabled.name(), "true",
                new BoltConnector( "bolt" ).advertised_address.name(), "127.0.0.1:7002" ) );
    }
}
