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
package org.neo4j.kernel.impl.locking;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Interrupted;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockAcquisitionTimeout;

import java.util.concurrent.TimeUnit;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.NonSensitiveException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.lock.ResourceType;

/**
 * Used in lock clients for cases when we unable to acquire a lock for a time that exceed configured
 * timeout, if any.
 *
 * @see LockManager.Client
 * @see GraphDatabaseSettings#lock_acquisition_timeout
 */
public class LockAcquisitionTimeoutException extends TransactionTerminatedException implements NonSensitiveException {

    private LockAcquisitionTimeoutException(
            ErrorGqlStatusObject gqlStatusObject, ResourceType resourceType, long resourceId, long timeoutNano) {
        super(
                gqlStatusObject,
                LockAcquisitionTimeout,
                String.format(
                        "Unable to acquire lock for resource: %s with id: %d within %d millis.",
                        resourceType, resourceId, TimeUnit.NANOSECONDS.toMillis(timeoutNano)));
    }

    private LockAcquisitionTimeoutException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        super(gqlStatusObject, status, message);
    }

    public static LockAcquisitionTimeoutException interrupted() {
        String reason = Interrupted.code().description() + " Interrupted while waiting";
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N16)
                .withParam(GqlParams.StringParam.msg, reason)
                .build();
        return new LockAcquisitionTimeoutException(gql, Interrupted, "Interrupted while waiting.");
    }

    public static LockAcquisitionTimeoutException lockAcquisitionTimeout(
            ResourceType resourceType, long resourceId, long timeoutNano) {

        String reason = LockAcquisitionTimeout.code().description()
                + String.format(
                        " Unable to acquire lock for resource: %s with id: %d within %d millis.",
                        resourceType, resourceId, TimeUnit.NANOSECONDS.toMillis(timeoutNano));
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N16)
                .withParam(GqlParams.StringParam.msg, reason)
                .build();
        return new LockAcquisitionTimeoutException(gql, resourceType, resourceId, timeoutNano);
    }
}
