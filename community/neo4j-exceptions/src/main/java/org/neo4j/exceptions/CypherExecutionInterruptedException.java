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
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.NonSensitiveException;
import org.neo4j.kernel.api.exceptions.Status;

public class CypherExecutionInterruptedException extends Neo4jException implements NonSensitiveException {
    private final Status status;

    private CypherExecutionInterruptedException(ErrorGqlStatusObject gqlStatusObject, String message, Status status) {
        super(gqlStatusObject, message);

        this.status = status;
    }

    public static CypherExecutionInterruptedException concurrentBatchTransactionInterrupted(Class<?> source) {
        String message = "The batch was interrupted and the transaction was rolled back because another batch failed";
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N16)
                .withParam(GqlParams.StringParam.msg, message)
                .build();
        return new CypherExecutionInterruptedException(
                gql, message, Status.Transaction.QueryExecutionFailedOnTransaction);
    }

    @Override
    public Status status() {
        return status;
    }
}
