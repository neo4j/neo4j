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
package org.neo4j.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

public class CypherExecutionInterruptedException extends Neo4jException {
    private final Status status;

    public CypherExecutionInterruptedException(String message, Status status) {
        super(message);
        this.status = status;
    }

    public CypherExecutionInterruptedException(ErrorGqlStatusObject gqlStatusObject, String message, Status status) {
        super(gqlStatusObject, message);

        this.status = status;
    }

    public static CypherExecutionInterruptedException concurrentBatchTransactionInterrupted() {
        return new CypherExecutionInterruptedException(
                "The batch was interrupted and the transaction was rolled back because another batch failed",
                Status.Transaction.QueryExecutionFailedOnTransaction);
    }

    public Status status() {
        return status;
    }
}
