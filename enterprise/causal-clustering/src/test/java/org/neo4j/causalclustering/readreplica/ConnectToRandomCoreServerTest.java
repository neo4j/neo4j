package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectToRandomCoreServerTest
{
    @Test
    public void shouldConnectToRandomCoreServer() throws Exception
    {
        // given

        MemberId memberId1 = new MemberId( UUID.randomUUID() );
        MemberId memberId2 = new MemberId( UUID.randomUUID() );
        MemberId memberId3 = new MemberId( UUID.randomUUID() );

        ReadReplicaTopologyService readReplicaTopologyService = mock( ReadReplicaTopologyService.class );
        when( readReplicaTopologyService.coreServers() )
                .thenReturn( fakeCoreTopology( memberId1, memberId2, memberId3 ) );

        ConnectToRandomCoreServer connectionStrategy = new ConnectToRandomCoreServer();
        connectionStrategy.setDiscoveryService( readReplicaTopologyService );

        // when
        MemberId memberId = connectionStrategy.upstreamDatabase().get();

        // then
        assertThat( memberId, anyOf( equalTo( memberId1 ), equalTo( memberId2 ), equalTo( memberId3 ) ) );
    }

    private CoreTopology fakeCoreTopology( MemberId... memberIds )
    {
        assert memberIds.length > 0;

        ClusterId clusterId = new ClusterId( UUID.randomUUID() );
        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            coreMembers.put( memberId, new CoreAddresses( new AdvertisedSocketAddress( "localhost", 5000 + offset ),
                    new AdvertisedSocketAddress( "localhost", 6000 + offset ), new ClientConnectorAddresses(
                    singletonList( new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 7000 + offset ) ) ) ) ) );

            offset++;
        }

        return new CoreTopology( clusterId, false, coreMembers );
    }
}
