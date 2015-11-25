/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.scenarios.explorer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.raft.state.explorer.ClusterSafetyViolations;
import org.neo4j.coreedge.raft.state.explorer.ClusterState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.state.explorer.action.Action;
import org.neo4j.coreedge.raft.state.explorer.action.DropMessage;
import org.neo4j.coreedge.raft.state.explorer.action.ElectionTimeout;
import org.neo4j.coreedge.raft.state.explorer.action.HeartbeatTimeout;
import org.neo4j.coreedge.raft.state.explorer.action.NewEntry;
import org.neo4j.coreedge.raft.state.explorer.action.ProcessMessage;
import org.neo4j.coreedge.raft.state.explorer.action.OutOfOrderDelivery;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.server.RaftTestMember.member;

public class StateExplorerIT
{
    @Test
    public void shouldExploreAllTheStates() throws Exception
    {
        int clusterSize = 3;
        Set<RaftTestMember> members = new HashSet<>();
        for ( int i = 0; i < clusterSize; i++ )
        {
            members.add( member( i ) );
        }

        ClusterState initialState = new ClusterState( members );

        List<Action> actions = new ArrayList<>();
        for ( RaftTestMember member : members )
        {
            actions.add( new ProcessMessage( member ) );
            actions.add( new NewEntry( member ) );
            actions.add( new HeartbeatTimeout( member ) );
            actions.add( new ElectionTimeout( member ) );
            actions.add( new DropMessage( member ) );
            actions.add( new OutOfOrderDelivery( member ) );
        }

        Set<ClusterState> exploredStates = new HashSet<>();
        Set<ClusterState> statesToBeExplored = new HashSet<>();

        statesToBeExplored.add( initialState );

        int explorationDepth = 7;
        for ( int i = 0; i < explorationDepth; i++ )
        {
            Set<ClusterState> newStates = new HashSet<>();
            int counter = 0;
            for ( ClusterState clusterState : statesToBeExplored )
            {
                if (counter++ % 1000 == 0) System.out.print( "." );
                exploredStates.add( clusterState );
                for ( Action action : actions )
                {
                    ClusterState nextClusterState = action.advance( clusterState );
                    if ( !exploredStates.contains( nextClusterState ) )
                    {
                        newStates.add( nextClusterState );
                    }
                }
            }
            statesToBeExplored = newStates;
            System.out.printf( "\nexplored %d states, planning to explore %d states%n",
                    exploredStates.size(), statesToBeExplored.size() );
        }

        for ( ClusterState exploredState : exploredStates )
        {
            List<ClusterSafetyViolations.Violation> invariantsViolated = ClusterSafetyViolations.violations(
                    exploredState );
            assertThat( invariantsViolated, empty() );
        }

        System.out.println( "exploredStates = " + exploredStates.size() );
    }
}
