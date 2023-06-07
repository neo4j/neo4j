/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.router.impl.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.QueryTargetService;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;

public record RouterTransactionContextImpl(
        TransactionInfo transactionInfo,
        RoutingInfo routingInfo,
        RouterTransaction routerTransaction,
        QueryTargetService queryTargetService,
        LocationService locationService,
        TransactionBookmarkManager txBookmarkManager)
        implements RouterTransactionContext {

    @Override
    public TransactionInfo transactionInfo() {
        return transactionInfo;
    }

    @Override
    public QueryTargetService queryTargetService() {
        return queryTargetService;
    }

    @Override
    public LocationService locationService() {
        return locationService;
    }

    @Override
    public DatabaseTransaction transactionFor(Location location) {
        return routerTransaction.transactionFor(location);
    }

    @Override
    public TransactionBookmarkManager txBookmarkManager() {
        return txBookmarkManager;
    }

    @Override
    public RouterTransaction routerTransaction() {
        return routerTransaction;
    }
}
