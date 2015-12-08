/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.tx.LastCommittedIndexFinder;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

/**
 * Retrieves last raft log index that was appended to the transaction log, so that raft log replay can recover while
 * preserving idempotency (avoid appending the same transaction twice).
 */
public class RecoverTransactionLogState extends LifecycleAdapter
{
    private final Dependencies dependencies;
    private final LogProvider logProvider;
    private final ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder;
    private final ReplicatedLabelTokenHolder labelTokenHolder;

    public RecoverTransactionLogState( Dependencies dependencies, LogProvider logProvider,
                                       ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder,
                                       ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder,
                                       ReplicatedLabelTokenHolder labelTokenHolder )
    {
        this.dependencies = dependencies;
        this.logProvider = logProvider;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
    }

    @Override
    public void start() throws Throwable
    {
        MetaDataStore metaDataStore = dependencies.resolveDependency( NeoStoreDataSource.class )
                .getNeoStores().getMetaDataStore();
        LogicalTransactionStore transactionStore = dependencies.resolveDependency( LogicalTransactionStore.class );

        long lastCommittedIndex = new LastCommittedIndexFinder( metaDataStore, transactionStore, logProvider )
                .getLastCommittedIndex();

        ReplicatedTransactionStateMachine replicatedTransactionStateMachine =
                dependencies.resolveDependency( ReplicatedTransactionStateMachine.class );

        replicatedTransactionStateMachine.setLastCommittedIndex( lastCommittedIndex );
        relationshipTypeTokenHolder.setLastCommittedIndex( lastCommittedIndex );
        propertyKeyTokenHolder.setLastCommittedIndex( lastCommittedIndex );
        labelTokenHolder.setLastCommittedIndex( lastCommittedIndex );
    }

}
