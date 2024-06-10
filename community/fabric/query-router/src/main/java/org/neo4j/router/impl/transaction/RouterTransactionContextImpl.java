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
package org.neo4j.router.impl.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.TargetService;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;

public record RouterTransactionContextImpl(
        TransactionInfo transactionInfo,
        RoutingInfo routingInfo,
        RouterTransaction routerTransaction,
        TargetService targetService,
        LocationService locationService,
        TransactionBookmarkManager txBookmarkManager,
        DatabaseTransaction sessionTransaction,
        boolean isRpcCall)
        implements RouterTransactionContext {

    @Override
    public TransactionInfo transactionInfo() {
        return transactionInfo;
    }

    @Override
    public TargetService targetService() {
        return targetService;
    }

    @Override
    public LocationService locationService() {
        return locationService;
    }

    @Override
    public DatabaseTransaction transactionFor(Location location, TransactionMode mode) {
        return routerTransaction.transactionFor(location, mode, locationService);
    }

    @Override
    public TransactionBookmarkManager txBookmarkManager() {
        return txBookmarkManager;
    }

    @Override
    public RouterTransaction routerTransaction() {
        return routerTransaction;
    }

    @Override
    public void verifyStatementType(StatementType type) {
        routerTransaction.verifyStatementType(type);
    }

    @Override
    public DatabaseReference sessionDatabaseReference() {
        return routingInfo.sessionDatabaseReference();
    }

    @Override
    public DatabaseTransaction sessionTransaction() {
        return sessionTransaction;
    }
}
