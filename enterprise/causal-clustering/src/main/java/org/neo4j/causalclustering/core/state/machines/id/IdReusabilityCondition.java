/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.util.Listener;

/**
 * Determines whether it is safe to reuse freed ids, based on current leader and tracking its own transactions.
 * This should guarantee that a single freed id only ends up on a single core.
 */
public class IdReusabilityCondition implements BooleanSupplier, Listener<MemberId>
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
    public void receive( MemberId newLeader )
    {
        if ( myself.equals( newLeader ) )
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
