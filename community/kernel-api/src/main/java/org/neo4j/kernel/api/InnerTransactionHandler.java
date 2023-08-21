/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api;

/**
 * This Handler provides the ability to link so-called inner transactions to the transaction this handler was obtained from using
 * {@link #registerInnerTransaction(long)} and to unlink them again using {@link #removeInnerTransaction(long)}.
 * <p>
 * For linked transactions the guarantees are:
 * <ul>
 *      <li>The inner transaction will be terminated if the outer transaction gets rolled back.</li>
 *      <li>The outer transaction cannot commit if it is linked to inner transactions.</li>
 * </ul>
 */
public interface InnerTransactionHandler {
    /**
     * Link the inner transaction specified by the given {@code innerTransactionId} to the transaction that this handler belongs to.
     * The inner transaction will be terminated if the outer transaction gets rolled back.
     * The outer transaction cannot commit if it is linked to inner transactions.
     *
     * @param innerTransactionId user transaction id of inner transaction
     */
    void registerInnerTransaction(long innerTransactionId);

    /**
     * Remove link to inner transaction.
     *
     * @param innerTransactionId user transaction id of inner transaction
     */
    void removeInnerTransaction(long innerTransactionId);
}
