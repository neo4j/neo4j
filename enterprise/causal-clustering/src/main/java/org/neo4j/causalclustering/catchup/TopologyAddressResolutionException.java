package org.neo4j.causalclustering.catchup;

import java.util.function.Supplier;

import org.neo4j.causalclustering.identity.MemberId;

public class TopologyAddressResolutionException extends RuntimeException // TODO TopologyLookupException also exists
{
    public TopologyAddressResolutionException( MemberId memberId )
    {
        super( String.format( "Unable to resolve address for member %s", memberId ) );
    }

    public static Supplier<TopologyAddressResolutionException> topologyAddressResolutionException(MemberId memberId) {
        return () -> new TopologyAddressResolutionException( memberId );
    }
}
