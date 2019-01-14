/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.identity.MemberId;

/**
 * Determines whether it is safe to reuse freed ids, based on current leader and tracking its own transactions.
 * This should guarantee that a single freed id only ends up on a single core.
 */
public class IdReusabilityCondition implements BooleanSupplier, LeaderListener
{
    private static final BooleanSupplier ALWAYS_FALSE = () -> false;

    private CommandIndexTracker commandIndexTracker;
    private final RaftMachine raftMachine;
    private final MemberId myself;

    private volatile BooleanSupplier currentSupplier = ALWAYS_FALSE;

    public IdReusabilityCondition( CommandIndexTracker commandIndexTracker, RaftMachine raftMachine, MemberId myself )
    {
        this.commandIndexTracker = commandIndexTracker;
        this.raftMachine = raftMachine;
        this.myself = myself;
        raftMachine.registerListener( this );
    }

    @Override
    public boolean getAsBoolean()
    {
        return currentSupplier.getAsBoolean();
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        if ( myself.equals( leaderInfo.memberId() ) )
        {
            // We just became leader
            currentSupplier = new LeaderIdReusabilityCondition( commandIndexTracker, raftMachine );
        }
        else
        {
            // We are not the leader
            currentSupplier = ALWAYS_FALSE;
        }
    }

    private static class LeaderIdReusabilityCondition implements BooleanSupplier
    {
        private final CommandIndexTracker commandIndexTracker;
        private final long commandIdWhenBecameLeader;

        private volatile boolean hasAppliedOldTransactions;

        LeaderIdReusabilityCondition( CommandIndexTracker commandIndexTracker, RaftMachine raftMachine )
        {
            this.commandIndexTracker = commandIndexTracker;

            // Get highest command id seen
            this.commandIdWhenBecameLeader = raftMachine.state().lastLogIndexBeforeWeBecameLeader();
        }

        @Override
        public boolean getAsBoolean()
        {
            // Once all transactions from previous term are applied we don't need to recheck with the CommandIndexTracker
            if ( !hasAppliedOldTransactions )
            {
                hasAppliedOldTransactions = commandIndexTracker.getAppliedCommandIndex() > commandIdWhenBecameLeader;
            }

            return hasAppliedOldTransactions;
        }
    }
}
