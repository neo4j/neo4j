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
package org.neo4j.causalclustering.diagnostics;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;

public class CoreMembershipMonitor implements MembershipWaiter.Monitor
{
    private final AtomicBoolean hasJoinedRaft = new AtomicBoolean( false );
    private final RaftMachine raftMachine;

    public CoreMembershipMonitor( RaftMachine raftMachine )
    {
        this.raftMachine = raftMachine;
    }

    @Override
    public void waitingToHearFromLeader()
    {
    }

    @Override
    public void waitingToCatchupWithLeader( long localCommitIndex, long leaderCommitIndex )
    {
        hasJoinedRaft.set( false );
    }

    @Override
    public void joinedRaftGroup()
    {
        hasJoinedRaft.set( true );
    }

    public boolean hasJoinedRaft()
    {
        return hasJoinedRaft.get() && isStableRaftState();
    }

    private boolean isStableRaftState()
    {
        try
        {
            raftMachine.getLeader();
            return true;
        }
        catch ( NoLeaderFoundException e )
        {
            return false;
        }
    }
}
