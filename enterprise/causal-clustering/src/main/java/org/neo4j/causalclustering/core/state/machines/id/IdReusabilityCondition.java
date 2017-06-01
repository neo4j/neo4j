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

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.Listener;

/**
 * Determines whether it is safe to reuse freed ids, based on current leader and tracking its own transactions.
 * This should guarantee that a single freed id only ends up on a single core.
 */
public class IdReusabilityCondition implements BooleanSupplier, Listener<MemberId>
{
    private static final BooleanSupplier ALWAYS_FALSE = () -> false;

    private TransactionIdStore transactionIdStore;
    private final Dependencies dependencies;
    private final MemberId myself;

    private volatile BooleanSupplier currentSupplier = ALWAYS_FALSE;

    public IdReusabilityCondition( Dependencies dependencies, LeaderLocator leaderLocator, MemberId myself )
    {
        this.dependencies = dependencies;
        this.myself = myself;
        leaderLocator.registerListener( this );
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
            resolveTransactionIdStore();
            currentSupplier = new LeaderIdReusabilityCondition( transactionIdStore );
        }
        else
        {
            // We are not the leader
            currentSupplier = ALWAYS_FALSE;
        }
    }

    private void resolveTransactionIdStore()
    {
        if ( transactionIdStore == null )
        {
            transactionIdStore = dependencies.provideDependency( TransactionIdStore.class ).get();
        }
    }

    private static class LeaderIdReusabilityCondition implements BooleanSupplier
    {
        private final TransactionIdStore transactionIdStore;
        private final long idHighestWhenBecameLeader;

        private volatile boolean oldTransactionsApplied;

        LeaderIdReusabilityCondition( TransactionIdStore transactionIdStore )
        {
            this.transactionIdStore = transactionIdStore;

            // Get highest transaction id seen
            this.idHighestWhenBecameLeader = transactionIdStore.getLastCommittedTransactionId();
        }

        @Override
        public boolean getAsBoolean()
        {
            if ( oldTransactionsApplied )
            {
                return true;
            }

            boolean hasAppliedOldTransactions = transactionIdStore.getLastClosedTransactionId() > idHighestWhenBecameLeader;
            if ( hasAppliedOldTransactions )
            {
                oldTransactionsApplied = true;
            }

            return hasAppliedOldTransactions;
        }
    }
}
