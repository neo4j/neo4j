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
package org.neo4j.kernel.impl.query;

import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.virtual.MapValue;

public interface ConstituentTransactionFactory {

    /**
     * Creates or gets a transaction for the given database reference
     *
     * @param databaseReference the database to create or get a transaction for
     * @return a transaction belonging to the database reference
     */
    ConstituentTransaction transactionFor(DatabaseReference databaseReference);

    static ConstituentTransactionFactory throwing() {
        return (DatabaseReference dbRef) -> {
            throw new InvalidSemanticsException(
                    "Multiple graph references in the same query is not supported on standard databases. This capability is supported on composite databases only.");
        };
    }

    /**
     * Transaction to a constituent database. This transaction will use the same configuration, such as transaction timeout, as the outer transaction.
     * It will be terminated/closed/committed, when the outer transaction is terminated/closed/committed.
     */
    interface ConstituentTransaction {

        /**
         * Executes the query with the given parameters.
         */
        QueryExecution executeQuery(String query, MapValue parameters, QuerySubscriber querySubscriber)
                throws QueryExecutionKernelException;
    }
}
