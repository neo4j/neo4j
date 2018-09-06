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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.DurationSinceLastMessageMonitor;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.causalclustering.ReadReplicaStatusTest.responseAsMap;

public class CoreStatusTest
{
    private CausalClusteringStatus status;

    private CoreGraphDatabase db;
    private Dependencies dependencyResolver = new Dependencies();
    private final LogProvider logProvider = NullLogProvider.getInstance();

    // Dependency resolved
    private RaftMembershipManager raftMembershipManager;
    private DatabaseHealth databaseHealth;
    private FakeTopologyService topologyService;
    private DurationSinceLastMessageMonitor raftMessageTimerResetMonitor;
    private RaftMachine raftMachine;
    private CommandIndexTracker commandIndexTracker;

    private final MemberId myself = new MemberId( new UUID( 0x1234, 0x5678 ) );
    private final MemberId core2 = new MemberId( UUID.randomUUID() );
    private final MemberId core3 = new MemberId( UUID.randomUUID() );
    private final MemberId replica = new MemberId( UUID.randomUUID() );

    @Before
    public void setup() throws Exception
    {
        OutputFormat output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ), null );
        db = mock( CoreGraphDatabase.class );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );

        raftMembershipManager = dependencyResolver.satisfyDependency( fakeRaftMembershipManager( new HashSet<>( Arrays.asList( myself, core2, core3 ) ) ) );

        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );

        topologyService = dependencyResolver.satisfyDependency(
                new FakeTopologyService( Arrays.asList( core2, core3 ), Collections.singleton( replica ), myself, RoleInfo.FOLLOWER ) );

        raftMessageTimerResetMonitor = dependencyResolver.satisfyDependency( new DurationSinceLastMessageMonitor() );
        raftMachine = dependencyResolver.satisfyDependency( mock( RaftMachine.class ) );
        commandIndexTracker = dependencyResolver.satisfyDependency( new CommandIndexTracker() );

        status = CausalClusteringStatusFactory.build( output, db );
    }

    @Test
    public void testAnswersWhenLeader()
    {
        // given
        when( db.getRole() ).thenReturn( Role.LEADER );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), readonly.getStatus() );
        assertEquals( "false", readonly.getEntity() );

        assertEquals( OK.getStatusCode(), writable.getStatus() );
        assertEquals( "true", writable.getEntity() );
    }

    @Test
    public void testAnswersWhenCandidate()
    {
        // given
        when( db.getRole() ).thenReturn( Role.CANDIDATE );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    public void testAnswersWhenFollower()
    {
        // given
        when( db.getRole() ).thenReturn( Role.FOLLOWER );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    public void expectedStatusFieldsAreIncluded() throws IOException, NoLeaderFoundException, InterruptedException
    {
        // given ideal normal conditions
        commandIndexTracker.setAppliedCommandIndex( 123 );
        when( raftMachine.getLeader() ).thenReturn( core2 );
        raftMessageTimerResetMonitor.timerReset();
        Thread.sleep( 1 ); // Sometimes the test can be fast. This guarantees at least 1 ms since message received

        // and helpers
        List<String> votingMembers =
                raftMembershipManager.votingMembers().stream().map( memberId -> memberId.getUuid().toString() ).sorted().collect( Collectors.toList() );

        // when
        Response description = status.description();
        Map<String,Object> response = responseAsMap( description );

        // then
        assertThat( response, containsAndEquals( "core", true ) );
        assertThat( response, containsAndEquals( "lastAppliedRaftIndex", 123 ) );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", true ) );
        assertThat( response, containsAndEquals( "votingMembers", votingMembers ) );
        assertThat( response, containsAndEquals( "healthy", true ) );
        assertThat( response, containsAndEquals( "memberId", myself.getUuid().toString() ) );
        assertThat( response, containsAndEquals( "leader", core2.getUuid().toString() ) );
        assertThat( response.toString(), Long.parseLong( response.get( "millisSinceLastLeaderMessage" ).toString() ), greaterThan( 0L ) );
    }

    @Test
    public void notParticipatingInRaftGroupWhenNotInVoterSet() throws IOException
    {
        // given not in voting set
        topologyService.replaceWithRole( core2, RoleInfo.LEADER );
        when( raftMembershipManager.votingMembers() ).thenReturn( new HashSet<>( Arrays.asList( core2, core3 ) ) );

        // when
        Response description = status.description();

        // then
        Map<String,Object> response = responseAsMap( description );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", false ) );
    }

    @Test
    public void notParticipatingInRaftGroupWhenLeaderUnknown() throws IOException
    {
        // given leader is unknown
        topologyService.replaceWithRole( null, RoleInfo.LEADER );

        // when
        Response description = status.description();

        // then
        Map<String,Object> response = responseAsMap( description );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", false ) );
    }

    @Test
    public void databaseHealthIsReflected() throws IOException
    {
        // given database is not healthy
        databaseHealth.panic( new RuntimeException() );

        // when
        Response description = status.description();
        Map<String,Object> response = responseAsMap( description );

        // then
        assertThat( response, containsAndEquals( "healthy", false ) );
    }

    @Test
    public void leaderNotIncludedIfUnknown() throws IOException
    {
        // given no leader
        topologyService.replaceWithRole( null, RoleInfo.LEADER );

        // when
        Response description = status.description();

        // then
        Map<String,Object> response = responseAsMap( description );
        assertFalse( description.getEntity().toString(), response.containsKey( "leader" ) );
    }

    static RaftMembershipManager fakeRaftMembershipManager( Set<MemberId> votingMembers )
    {
        RaftMembershipManager raftMembershipManager = mock( RaftMembershipManager.class );
        when( raftMembershipManager.votingMembers() ).thenReturn( votingMembers );
        return raftMembershipManager;
    }

    private static Matcher<Map<String,Object>> containsAndEquals( String key, Object target )
    {
        return new BaseMatcher<Map<String,Object>>()
        {
            private boolean containsKey;
            private boolean areEqual;

            @Override
            public boolean matches( Object item )
            {
                Map<String,Object> map = (Map<String,Object>) item;
                if ( !map.containsKey( key ) )
                {
                    return false;
                }
                containsKey = true;
                if ( !map.get( key ).equals( target ) )
                {
                    return false;
                }
                areEqual = true;
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                if ( !containsKey )
                {
                    description.appendText( "did not include key " ).appendValue( key );
                }
                else if ( !areEqual )
                {
                    description.appendText( "key " ).appendValue( key ).appendText( " did not match value" ).appendValue( target );
                }
                else
                {
                    throw new IllegalStateException( "Matcher failed, conditions should have passed" );
                }
            }
        };
    }
}
