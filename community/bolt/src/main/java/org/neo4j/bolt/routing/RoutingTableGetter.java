/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.routing;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.values.virtual.MapValue;

/**
 * Interface which provides the routing table given a routing context and a database name.
 */
public interface RoutingTableGetter
{
    /**
     * Retrieves the routing table
     *
     * @param transactionManager The transaction manager which will be used to get the routing information.
     * @param routingContext Meta information used by the routing procedure to create the routing table.
     * @param bookmarks the bookmark required to wait for before executing.
     * @param databaseName The name of the database from the routing table will be get.
     * @param connectionId the connectionId which requested the routing table.
     * @return A promise of a routing table
     */
    CompletableFuture<MapValue> get( TransactionManager transactionManager, MapValue routingContext, List<Bookmark> bookmarks,
                                     String databaseName, String connectionId );
}
