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
package org.neo4j.kernel.api.txstate;

import java.util.Map;

import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.state.RecordState;

/**
 * Defines transactional state for legacy indexes. Since the implementation of this enlists another transaction
 * management engine under the hood, these methods have been split out from
 * {@link TransactionState the transaction state} in order to be able to keep the implementation of
 * {@link org.neo4j.kernel.impl.api.state.TxState transaction state} simple with no dependencies.
 */
public interface LegacyIndexTransactionState extends RecordState
{
    void clear();

    LegacyIndex nodeChanges( String indexName ) throws LegacyIndexNotFoundKernelException;

    LegacyIndex relationshipChanges( String indexName ) throws LegacyIndexNotFoundKernelException;

    void createIndex( IndexEntityType node, String name, Map<String, String> config );

    void deleteIndex( IndexEntityType entityType, String indexName );
}
