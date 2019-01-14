/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.locking;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * Used in lock clients for cases when we unable to acquire a lock for a time that exceed configured
 * timeout, if any.
 *
 * @see Locks.Client
 * @see GraphDatabaseSettings#lock_acquisition_timeout
 */
public class LockAcquisitionTimeoutException extends TransactionTerminatedException
{
    public LockAcquisitionTimeoutException( ResourceType resourceType, long resourceId, long timeoutMillis )
    {
        super( Status.Transaction.LockAcquisitionTimeout,
                String.format( "Unable to acquire lock for resource: %s with id: %d within %d millis.", resourceType,
                        resourceId, timeoutMillis ) );
    }
}
