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

import java.util.concurrent.TimeUnit;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
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
public class LockAcquisitionTimeoutException extends TransactionTerminatedException {
    public LockAcquisitionTimeoutException(ResourceType resourceType, long resourceId, long timeoutNano) {
        super(
                Status.Transaction.LockAcquisitionTimeout,
                String.format(
                        "Unable to acquire lock for resource: %s with id: %d within %d millis.",
                        resourceType, resourceId, TimeUnit.NANOSECONDS.toMillis(timeoutNano)));
    }

    public LockAcquisitionTimeoutException(
            ErrorGqlStatusObject gqlStatusObject, ResourceType resourceType, long resourceId, long timeoutNano) {
        super(
                gqlStatusObject,
                Status.Transaction.LockAcquisitionTimeout,
                String.format(
                        "Unable to acquire lock for resource: %s with id: %d within %d millis.",
                        resourceType, resourceId, TimeUnit.NANOSECONDS.toMillis(timeoutNano)));
    }

    public LockAcquisitionTimeoutException(Status status, String message) {
        super(status, message);
    }

    public LockAcquisitionTimeoutException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        super(gqlStatusObject, status, message);
    }
}
