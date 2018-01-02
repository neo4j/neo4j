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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache.TransactionMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;

/**
 * Accessor of meta data information about transactions.
 */
public interface LogicalTransactionStore
{
    /**
     * Acquires a {@link TransactionCursor cursor} which will provide {@link CommittedTransactionRepresentation}
     * instances for committed transactions, starting from the specified {@code transactionIdToStartFrom}.
     * Transactions will be returned from the cursor in transaction-id-sequential order.
     *
     * @param transactionIdToStartFrom id of the first transaction that the cursor will return.
     * @return an {@link TransactionCursor} capable of returning {@link CommittedTransactionRepresentation} instances
     * for committed transactions, starting from the specified {@code transactionIdToStartFrom}.
     * @throws NoSuchTransactionException if the requested transaction hasn't been committed,
     * or if the transaction has been committed, but information about it is no longer available for some reason.
     * @throws IOException if there was an I/O related error looking for the start transaction.
     */
    TransactionCursor getTransactions( long transactionIdToStartFrom )
            throws NoSuchTransactionException, IOException;

    /**
     * Acquires a {@link TransactionCursor cursor} which will provide {@link CommittedTransactionRepresentation}
     * instances for committed transactions, starting from the specified {@link LogPosition}.
     * This is useful for placing a cursor at a position referred to by a {@link CheckPoint}.
     * Transactions will be returned from the cursor in transaction-id-sequential order.
     *
     * @param position {@link LogPosition} of the first transaction that the cursor will return.
     * @return an {@link TransactionCursor} capable of returning {@link CommittedTransactionRepresentation} instances
     * for committed transactions, starting from the specified {@code position}.
     * @throws NoSuchTransactionException if the requested transaction hasn't been committed,
     * or if the transaction has been committed, but information about it is no longer available for some reason.
     * @throws IOException if there was an I/O related error looking for the start transaction.
     */
    TransactionCursor getTransactions( LogPosition position )
            throws NoSuchTransactionException, IOException;

    /**
     * Looks up meta data about a committed transaction.
     *
     * @param transactionId id of the transaction to look up meta data for.
     * @return {@link TransactionMetadata} containing meta data about the specified transaction.
     * @throws NoSuchTransactionException if the requested transaction hasn't been committed,
     * or if the transaction has been committed, but information about it is no longer available for some reason.
     * @throws IOException if there was an I/O related error during reading the meta data.
     */
    TransactionMetadataCache.TransactionMetadata getMetadataFor( long transactionId )
            throws NoSuchTransactionException, IOException;
}
