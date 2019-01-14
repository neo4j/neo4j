/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.state.RecordState;

/**
 * Defines transactional state for explicit indexes. Since the implementation of this enlists another transaction
 * management engine under the hood, these methods have been split out from
 * {@link TransactionState the transaction state} in order to be able to keep the implementation of
 * {@link org.neo4j.kernel.impl.api.state.TxState transaction state} simple with no dependencies.
 */
public interface ExplicitIndexTransactionState extends RecordState
{
    ExplicitIndex nodeChanges( String indexName ) throws ExplicitIndexNotFoundKernelException;

    ExplicitIndex relationshipChanges( String indexName ) throws ExplicitIndexNotFoundKernelException;

    void createIndex( IndexEntityType entityType, String indexName, Map<String, String> config );

    void deleteIndex( IndexEntityType entityType, String indexName );

    /**
     * Checks whether or not index with specific {@code name} exists.
     * Optionally the specific {@code config} is verified to be matching.
     *
     * This method can either return {@code boolean} or {@code throw} exception on:
     * <ul>
     * <li>index exists, config is provided and matching => {@code true}</li>
     * <li>index exists, config is provided and NOT matching => {@code throw exception}</li>
     * <li>index exists, config is NOT provided => {@code true}</li>
     * <li>index does NOT exist => {@code false}</li>
     * </ul>
     *
     * @param entityType {@link IndexEntityType} for the index.
     * @param indexName name of the index.
     * @param config configuration which must match the existing index, if it exists. {@code null} means
     * that the configuration doesn't need to be checked.
     * @return {@code true} if the index with the specific {@code name} and {@code entityType} exists, otherwise {@code false}.
     */
    boolean checkIndexExistence( IndexEntityType entityType, String indexName, Map<String, String> config );
}
