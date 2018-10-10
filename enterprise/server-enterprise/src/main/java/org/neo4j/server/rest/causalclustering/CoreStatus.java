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
package org.neo4j.server.rest.causalclustering;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.core.Response;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.DurationSinceLastMessageMonitor;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.server.rest.repr.OutputFormat;

import static org.neo4j.server.rest.causalclustering.CausalClusteringService.BASE_PATH;

class CoreStatus extends BaseStatus
{
    private final OutputFormat output;
    private final CoreGraphDatabase db;

    // Dependency resolved
    private final RaftMembershipManager raftMembershipManager;
    private final DatabaseHealth databaseHealth;
    private final TopologyService topologyService;
    private final DurationSinceLastMessageMonitor raftMessageTimerResetMonitor;
    private final RaftMachine raftMachine;
    private final CommandIndexTracker commandIndexTracker;

    CoreStatus( OutputFormat output, CoreGraphDatabase db )
    {
        super( output );
        this.output = output;
        this.db = db;

        DependencyResolver dependencyResolver = db.getDependencyResolver();
        this.raftMembershipManager = dependencyResolver.resolveDependency( RaftMembershipManager.class );
        this.databaseHealth = dependencyResolver.resolveDependency( DatabaseHealth.class );
        this.topologyService = dependencyResolver.resolveDependency( TopologyService.class );
        this.raftMachine = dependencyResolver.resolveDependency( RaftMachine.class );
        this.raftMessageTimerResetMonitor = dependencyResolver.resolveDependency( DurationSinceLastMessageMonitor.class );
        commandIndexTracker = dependencyResolver.resolveDependency( CommandIndexTracker.class );
    }

    @Override
    public Response discover()
    {
        return output.ok( new CausalClusteringDiscovery( BASE_PATH ) );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        Role role = db.getRole();
        return Arrays.asList( Role.FOLLOWER, Role.CANDIDATE ).contains( role ) ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response writable()
    {
        return db.getRole() == Role.LEADER ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response description()
    {
        MemberId myself = topologyService.myself();
        MemberId leader = getLeader();
        List<MemberId> votingMembers = new ArrayList<>( raftMembershipManager.votingMembers() );
        boolean participatingInRaftGroup = votingMembers.contains( myself ) && Objects.nonNull( leader );

        long lastAppliedRaftIndex = commandIndexTracker.getAppliedCommandIndex();
        final Duration millisSinceLastLeaderMessage;
        if ( myself.equals( leader ) )
        {
            millisSinceLastLeaderMessage = Duration.ofMillis( 0 );
        }
        else
        {
            millisSinceLastLeaderMessage = raftMessageTimerResetMonitor.durationSinceLastMessage();
        }

        return statusResponse( lastAppliedRaftIndex, participatingInRaftGroup, votingMembers, databaseHealth.isHealthy(), myself, leader,
                millisSinceLastLeaderMessage, true );
    }

    private MemberId getLeader()
    {
        try
        {
            return raftMachine.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            return null;
        }
    }
}
