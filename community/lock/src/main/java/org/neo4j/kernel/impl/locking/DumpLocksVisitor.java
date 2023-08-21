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

import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.logging.InternalLog;

public class DumpLocksVisitor implements LockManager.Visitor {
    private final InternalLog log;

    public DumpLocksVisitor(InternalLog log) {
        this.log = log;
    }

    @Override
    public void visit(
            LockType lockType,
            ResourceType resourceType,
            long transactionId,
            long resourceId,
            String description,
            long estimatedWaitTime,
            long lockIdentityHashCode) {
        log.info(
                "%s{id=%d, txId=%d, waitTime=%d, description=%s, lockHash=%d}",
                resourceType, resourceId, transactionId, estimatedWaitTime, description, lockIdentityHashCode);
    }
}
