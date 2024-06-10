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
package org.neo4j.router.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.TargetService;

/**
 * Context object that contains transaction-specific information and services
 */
public interface RouterTransactionContext {

    TransactionInfo transactionInfo();

    TargetService targetService();

    LocationService locationService();

    /**
     * Begins a new, or reuses an existing, database transaction for the given location
     */
    DatabaseTransaction transactionFor(Location location, TransactionMode mode);

    TransactionBookmarkManager txBookmarkManager();

    RouterTransaction routerTransaction();

    void verifyStatementType(StatementType type);

    DatabaseReference sessionDatabaseReference();

    DatabaseTransaction sessionTransaction();

    boolean isRpcCall();
}
