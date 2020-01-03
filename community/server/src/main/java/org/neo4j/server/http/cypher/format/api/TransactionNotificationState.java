/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher.format.api;

/**
 * A state of the transaction used by the request at the end of the request execution and as presented by {@link TransactionInfoEvent} to the user.
 */
public enum TransactionNotificationState
{
    /**
     * There was no transaction used by during the request processing.
     * <p>
     * This is typically a state when some kind of error occurred when starting a transaction or obtaining an existing one.
     */
    NO_TRANSACTION,
    /**
     * The transaction is open and ready to process more requests.
     */
    OPEN,
    /**
     * The transaction has been successfully committed.
     */
    COMMITTED,
    /**
     * The transaction has been successfully rolled back.
     */
    ROLLED_BACK,
    /**
     * The transaction is in unknown state. This is typically a state when some kind of error occurred when committing or rolling back the transaction.
     */
    UNKNOWN
}
