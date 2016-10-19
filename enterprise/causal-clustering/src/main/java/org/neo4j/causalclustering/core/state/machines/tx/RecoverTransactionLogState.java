/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.tx;

import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

/**
 * Retrieves last raft log index that was appended to the transaction log, so that raft log replay can recover while
 * preserving idempotency (avoid appending the same transaction twice).
 */
public class RecoverTransactionLogState
{
    private final Dependencies dependencies;
    private final LogProvider logProvider;

    public RecoverTransactionLogState( Dependencies dependencies, LogProvider logProvider )
    {
        this.dependencies = dependencies;
        this.logProvider = logProvider;
    }

    public long findLastAppliedIndex()
    {
        TransactionIdStore transactionIdStore = dependencies.resolveDependency( TransactionIdStore.class );
        LogicalTransactionStore transactionStore = dependencies.resolveDependency( LogicalTransactionStore.class );

        return new LastCommittedIndexFinder( transactionIdStore, transactionStore, logProvider )
                .getLastCommittedIndex();
    }
}
