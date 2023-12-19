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

import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

/**
 * Retrieves last raft log index that was appended to the transaction log, so that raft log replay can recover while
 * preserving idempotency (avoid appending the same transaction twice).
 */
public class RecoverConsensusLogIndex
{
    private final Dependencies dependencies;
    private final LogProvider logProvider;

    public RecoverConsensusLogIndex( Dependencies dependencies, LogProvider logProvider )
    {
        this.dependencies = dependencies;
        this.logProvider = logProvider;
    }

    public long findLastAppliedIndex()
    {
        TransactionIdStore transactionIdStore = dependencies.resolveDependency( TransactionIdStore.class, ONLY );
        LogicalTransactionStore transactionStore = dependencies.resolveDependency( LogicalTransactionStore.class, ONLY );

        return new LastCommittedIndexFinder( transactionIdStore, transactionStore, logProvider )
                .getLastCommittedIndex();
    }
}
