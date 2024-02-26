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
package org.neo4j.procedure.builtin;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public final class TransactionId {
    private static final String SEPARATOR = "-transaction-";
    private static final String EXPECTED_FORMAT_MSG = "(expected format: <databasename>" + SEPARATOR + "<id>)";

    private TransactionId() {}

    public static String formatTransactionId(String database, long internalId) throws InvalidArgumentsException {
        if (internalId < 0) {
            throw new InvalidArgumentsException("Negative ids are not supported " + EXPECTED_FORMAT_MSG);
        }
        var normalizedDatabaseName = new NormalizedDatabaseName(database);
        return normalizedDatabaseName.name() + SEPARATOR + internalId;
    }
}
