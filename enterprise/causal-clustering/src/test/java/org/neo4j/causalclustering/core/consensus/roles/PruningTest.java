/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.PruneLogCommand;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( Parameterized.class )
public class PruningTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER}, {Role.LEADER}, {Role.CANDIDATE}
        } );
    }

    @Parameterized.Parameter
    public Role role;

    private MemberId myself = member( 0 );

    @Test
    public void shouldGeneratePruneCommandsOnRequest() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        // when
        RaftMessages.PruneRequest pruneRequest = new RaftMessages.PruneRequest( 1000 );
        Outcome outcome = role.handler.handle( pruneRequest, state, log() );

        // then
        assertThat( outcome.getLogCommands(), hasItem( new PruneLogCommand( 1000 ) ) );
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
