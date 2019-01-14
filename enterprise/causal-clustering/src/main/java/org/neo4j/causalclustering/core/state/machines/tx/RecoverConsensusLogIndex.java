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
