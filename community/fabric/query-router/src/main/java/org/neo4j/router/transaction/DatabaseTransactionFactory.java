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

import java.util.function.Consumer;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.router.location.LocationService;

/**
 * A factory for starting database transactions at given locations
 */
public interface DatabaseTransactionFactory<LOC extends Location> {

    DatabaseTransaction beginTransaction(
            LOC location,
            TransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager,
            Consumer<Status> terminationCallback,
            ConstituentTransactionFactory constituentTransactionFactory);

    default void addSpdInformationToTransaction(
            InternalTransaction internalTransaction, LocationService locationService, Location location) {
        throw new RuntimeException("Sharded property databases not supported in community edition.");
    }
}
