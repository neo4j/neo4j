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
package org.neo4j.causalclustering.core.state.machines.tx;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

/**
 * Counts transactions, and only applies new transactions once it has already seen enough transactions to reproduce
 * the current state of the store.
 */
class ReplayableCommitProcess implements TransactionCommitProcess
{
    private final AtomicLong lastLocalTxId = new AtomicLong( 1 );
    private final TransactionCommitProcess localCommitProcess;
    private final TransactionCounter transactionCounter;

    ReplayableCommitProcess( TransactionCommitProcess localCommitProcess, TransactionCounter transactionCounter )
    {
        this.localCommitProcess = localCommitProcess;
        this.transactionCounter = transactionCounter;
    }

    @Override
    public long commit( TransactionToApply batch,
                        CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        long txId = lastLocalTxId.incrementAndGet();
        if ( txId > transactionCounter.lastCommittedTransactionId() )
        {
            return localCommitProcess.commit( batch, commitEvent, mode );
        }
        return txId;
    }
}
