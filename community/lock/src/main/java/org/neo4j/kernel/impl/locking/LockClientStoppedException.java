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

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Exception thrown when stopped {@link LockManager.Client} used to acquire locks.
 */
public class LockClientStoppedException extends TransactionTerminatedException {
    public LockClientStoppedException(LockManager.Client client) {
        super(Status.Transaction.LockClientStopped, String.valueOf(client));
    }

    public LockClientStoppedException(ErrorGqlStatusObject gqlStatusObject, LockManager.Client client) {
        super(gqlStatusObject, Status.Transaction.LockClientStopped, String.valueOf(client));
    }
}
