/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.net.URI;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;

public class CommitProcessSwitcher extends AbstractModeSwitcher<TransactionCommitProcess>
{
    private final LogicalTransactionStore logicalTransactionStore;
    private final KernelHealth kernelHealth;
    private final NeoStore neoStore;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final AvailabilityGuard availabilityGuard;

    public CommitProcessSwitcher( LogicalTransactionStore logicalTransactionStore, KernelHealth kernelHealth,
                                  NeoStore neoStore, TransactionRepresentationStoreApplier storeApplier,
                                  DelegateInvocationHandler<Master> master,
                                  DelegateInvocationHandler<TransactionCommitProcess> delegate,
                                  RequestContextFactory requestContextFactory, HighAvailabilityMemberStateMachine
            memberStateMachine, AvailabilityGuard availabilityGuard )
    {
        super( memberStateMachine, delegate );
        this.logicalTransactionStore = logicalTransactionStore;
        this.kernelHealth = kernelHealth;
        this.neoStore = neoStore;
        this.storeApplier = storeApplier;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
    }

    @Override
    protected TransactionCommitProcess getSlaveImpl( URI serverHaUri )
    {
        return new SlaveTransactionCommitProcess( master.cement(), requestContextFactory, logicalTransactionStore,
                kernelHealth, neoStore, storeApplier, false );
    }

    @Override
    protected TransactionCommitProcess getMasterImpl()
    {
        return new TransactionRepresentationCommitProcess(logicalTransactionStore, kernelHealth, neoStore, storeApplier,
                false );
    }
}
