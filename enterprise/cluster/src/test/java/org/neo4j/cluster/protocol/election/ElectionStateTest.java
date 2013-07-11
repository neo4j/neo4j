/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.protocol.election;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.protocol.election.ElectionMessage.demote;
import static org.neo4j.cluster.protocol.election.ElectionMessage.performRoleElections;
import static org.neo4j.cluster.protocol.election.ElectionState.election;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.omega.MessageArgumentMatcher;
import org.neo4j.kernel.impl.util.StringLogger;

public class ElectionStateTest
{
    @Test
    public void testElectionRequestIsRejectedIfNoQuorum() throws Throwable
    {
        ElectionContext context = mock( ElectionContext.class );
        when( context.electionOk() ).thenReturn( false );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context,
                Message.<ElectionMessage>internal( performRoleElections ), holder );

        verifyZeroInteractions( holder );
    }

    @Test
    public void testElectionFromDemoteIsRejectedIfNoQuorum() throws Throwable
    {
        ElectionContext context = mock( ElectionContext.class );
        when( context.electionOk() ).thenReturn( false );

        MessageHolder holder = mock( MessageHolder.class );

        election.handle( context,
                Message.<ElectionMessage>internal( demote ), holder );

        verifyZeroInteractions( holder );
    }

    @Test
    public void electionShouldRemainLocalIfStartedBySingleInstanceWhichIsTheRoleHolder() throws Throwable
    {
        /*
         * Ensures that when an instance is alone in the cluster, elections for roles that it holds do not set
         * timeouts or try to reach other instances.
         */

        // Given
        ElectionContext context = mock( ElectionContext.class );
        MessageHolder holder = mock( MessageHolder.class );

          // These mean the election can proceed normally, by us
        when( context.electionOk() ).thenReturn( true );
        when( context.isInCluster() ).thenReturn( true );
        when( context.isElector() ).thenReturn( true );

          // Like it says on the box, we are the only instance
        final InstanceId myInstanceId = new InstanceId( 1 );
        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( myInstanceId, URI.create( "ha://me" ) );
        when( context.getMembers() ).thenReturn( members );

          // Any role would do, just make sure we have it
        final String role = "master";
        when( context.getPossibleRoles() ).thenReturn(
                Collections.<ElectionRole>singletonList( new ElectionRole( role ) ) );
        when( context.getElected( role ) ).thenReturn( myInstanceId );

          // Required for logging
        when( context.getLogger() ).thenReturn( mock( StringLogger.class ) );

        // When
        election.handle( context,
                Message.<ElectionMessage>internal( performRoleElections ), holder );

        // Then
          // Make sure that we asked ourselves to vote for that role and that no timer was set
        verify( holder, times(1) ).offer( Matchers.argThat( new MessageArgumentMatcher<ElectionMessage>()
                .onMessageType( ElectionMessage.vote ).withPayload( role ) ) );
        verify( context, times( 0 ) ).setTimeout( Matchers.<String>any(), Matchers.<Message>any() );
    }
}
