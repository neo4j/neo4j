/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.transaction;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * Represents the state of a transaction from the view of the {@link TransactionManager}.
 */
public class TransactionStatus {
    private final Value value;
    private Status error;

    public TransactionStatus(Value transactionStatus) {
        this.value = transactionStatus;
    }

    public TransactionStatus(Value transactionStatus, Status error) {
        this.value = transactionStatus;
        this.error = error;
    }

    public Value value() {
        return value;
    }

    public Status error() {
        return error;
    }

    public enum Value {
        /**
         * The transaction is either closed or does not exist.
         */
        CLOSED_OR_DOES_NOT_EXIST,
        /**
         * The transaction is in progress but does not currently have any open statements.
         */
        IN_TRANSACTION_NO_OPEN_STATEMENTS,
        /**
         * The transaction is in progress with open statements.
         */
        IN_TRANSACTION_OPEN_STATEMENT,
        /**
         * The transaction has been interrupted and is in the process of terminating.
         */
        INTERRUPTED
    }
}
