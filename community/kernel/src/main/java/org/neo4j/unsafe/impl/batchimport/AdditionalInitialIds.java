/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

/**
 * Migrating a store uses the {@link ParallelBatchImporter} to do so, where node/relationship stores
 * are created with data read from legacy node/relationship stores. The batch import also populates
 * a counts store, which revolves around tokens and their ids. Knowing those high token ids before hand greatly helps
 * the batch importer code do things efficiently, instead of figuring that out as it goes. When doing
 * the migration there are no token stores, although nodes and relationships gets importer with existing
 * token ids in them, so this is a way for the {@link StoreMigrator} to communicate those ids to the
 * {@link ParallelBatchImporter}.
 *
 * When actually writing out the counts store on disk the last committed transaction id at that point is also
 * stored, and that's why the {@link StoreMigrator} needs to communicate that using
 * {@link #lastCommittedTransactionId()} as well.
 */
public interface AdditionalInitialIds
{
    int highLabelTokenId();

    int highRelationshipTypeTokenId();

    int highPropertyKeyTokenId();

    long lastCommittedTransactionId();

    long lastCommittedTransactionChecksum();

    long lastCommittedTransactionLogVersion();

    long lastCommittedTransactionLogByteOffset();

    /**
     * High ids of zero, useful when creating a completely new store with {@link ParallelBatchImporter}.
     */
    AdditionalInitialIds EMPTY = new AdditionalInitialIds()
    {
        @Override
        public int highRelationshipTypeTokenId()
        {
            return 0;
        }

        @Override
        public int highPropertyKeyTokenId()
        {
            return 0;
        }

        @Override
        public int highLabelTokenId()
        {
            return 0;
        }

        @Override
        public long lastCommittedTransactionId()
        {
            return TransactionIdStore.BASE_TX_ID;
        }

        @Override
        public long lastCommittedTransactionChecksum()
        {
            return TransactionIdStore.BASE_TX_CHECKSUM;
        }

        @Override
        public long lastCommittedTransactionLogVersion()
        {
            return TransactionIdStore.BASE_TX_LOG_VERSION;
        }

        @Override
        public long lastCommittedTransactionLogByteOffset()
        {
            return TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
        }
    };
}
