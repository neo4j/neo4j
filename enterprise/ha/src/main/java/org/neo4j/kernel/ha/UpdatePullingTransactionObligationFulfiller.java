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
package org.neo4j.kernel.ha;

import java.util.function.Supplier;

import org.neo4j.cluster.InstanceId;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Fulfills transaction obligations by poking {@link UpdatePuller} and awaiting it to commit and apply
 * the desired transactions.
 */
public class UpdatePullingTransactionObligationFulfiller extends LifecycleAdapter
        implements TransactionObligationFulfiller
{
    private final UpdatePuller updatePuller;
    private final RoleListener listener;
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;

    private volatile TransactionIdStore transactionIdStore;

    public UpdatePullingTransactionObligationFulfiller( UpdatePuller updatePuller,
            HighAvailabilityMemberStateMachine memberStateMachine, InstanceId serverId,
            Supplier<TransactionIdStore> transactionIdStoreSupplier )
    {
        this.updatePuller = updatePuller;
        this.memberStateMachine = memberStateMachine;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.listener = new RoleListener( serverId );
    }

    /**
     * Triggers pulling of updates up until at least {@code toTxId} if no pulling is currently happening
     * and returns immediately.
     */
    @Override
    public void fulfill( final long toTxId ) throws InterruptedException
    {
        updatePuller.pullUpdates( ( currentTicket, targetTicket ) ->
        {
            /*
             * We need to await last *closed* transaction id, not last *committed* transaction id since
             * right after leaving this method we might read records off of disk, and they had better
             * be up to date, otherwise we read stale data.
             */
            return transactionIdStore != null &&
                   transactionIdStore.getLastClosedTransactionId() >= toTxId;
        }, true /*We strictly need the update puller to be and remain active while we wait*/ );
    }

    @Override
    public void start()
    {
        memberStateMachine.addHighAvailabilityMemberListener( listener );
    }

    @Override
    public void stop()
    {
        memberStateMachine.removeHighAvailabilityMemberListener( listener );
    }

    private class RoleListener extends HighAvailabilityMemberListener.Adapter
    {
        private final InstanceId myInstanceId;

        private RoleListener( InstanceId myInstanceId )
        {
            this.myInstanceId = myInstanceId;
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getInstanceId().equals( myInstanceId ) )
            {
                // I'm a slave, let the transactions stream in

                // Pull out the transaction id store at this very moment, because we receive this event
                // when joining a cluster or switching to a new master and there might have been a store copy
                // just now where there has been a new transaction id store created.
                transactionIdStore = transactionIdStoreSupplier.get();
            }
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            // clear state to avoid calling out of date objects
            transactionIdStore = null;
        }
    }
}
