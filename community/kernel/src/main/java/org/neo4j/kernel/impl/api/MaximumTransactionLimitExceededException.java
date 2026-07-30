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
package org.neo4j.kernel.impl.api;

import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.NonSensitiveException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;

public class MaximumTransactionLimitExceededException extends TransactionFailureException
        implements Status.HasStatus, NonSensitiveException {
    private static final String MAXIMUM_TRANSACTIONS_LIMIT_MESSAGE =
            "Unable to start new transaction since limit of concurrently executed transactions is reached. See setting "
                    + max_concurrent_transactions.name();

    private MaximumTransactionLimitExceededException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject, MAXIMUM_TRANSACTIONS_LIMIT_MESSAGE, Status.Transaction.MaximumTransactionLimitReached);
    }

    public static MaximumTransactionLimitExceededException maximumNumberOfTransactionsExceeded() {
        // KNL-012
        var gqlStatusObject = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N74)
                .withParam(GqlParams.StringParam.cfgSetting, max_concurrent_transactions.name())
                .build();
        return new MaximumTransactionLimitExceededException(gqlStatusObject);
    }
}
