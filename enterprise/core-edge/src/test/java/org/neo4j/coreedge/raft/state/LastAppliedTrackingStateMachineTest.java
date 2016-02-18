/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.state;

import org.junit.Test;

import org.neo4j.coreedge.raft.ReplicatedInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.raft.state.LastAppliedTrackingStateMachine.NOTHING_APPLIED;

public class LastAppliedTrackingStateMachineTest
{
    @Test
    public void shouldInitiallyHaveNothingApplied() throws Exception
    {
        // given
        LastAppliedTrackingStateMachine lastAppliedTrackingStateMachine = new LastAppliedTrackingStateMachine( mock( StateMachines.class ) );

        // then
        assertEquals( NOTHING_APPLIED, lastAppliedTrackingStateMachine.lastApplied() );
    }

    @Test
    public void shouldKeepTrackOfTheLastAppliedIndex() throws Exception
    {
        // given
        LastAppliedTrackingStateMachine lastAppliedTrackingStateMachine = new LastAppliedTrackingStateMachine( mock( StateMachines.class ) );

        // when
        lastAppliedTrackingStateMachine.applyCommand( ReplicatedInteger.valueOf( 1 ), 1 );

        // then
        assertEquals( 1, lastAppliedTrackingStateMachine.lastApplied() );
    }

    @Test
    public void shouldKeepHighestIndexSeenSoFar() throws Exception
    {
        // given
        LastAppliedTrackingStateMachine lastAppliedTrackingStateMachine = new LastAppliedTrackingStateMachine( mock( StateMachines.class ) );

        // when
        lastAppliedTrackingStateMachine.applyCommand( ReplicatedInteger.valueOf( 1 ), 9 );
        lastAppliedTrackingStateMachine.applyCommand( ReplicatedInteger.valueOf( 1 ), 1 );

        // then
        assertEquals( 9, lastAppliedTrackingStateMachine.lastApplied() );
    }
}