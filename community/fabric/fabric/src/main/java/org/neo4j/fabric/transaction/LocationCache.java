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
package org.neo4j.fabric.transaction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.Location;

public class LocationCache {
    private final CatalogManager catalogManager;
    private final FabricTransactionInfo transactionInfo;

    private final Map<Catalog.Graph, Location> locationMap = new ConcurrentHashMap<>();

    public LocationCache(CatalogManager catalogManager, FabricTransactionInfo transactionInfo) {
        this.catalogManager = catalogManager;
        this.transactionInfo = transactionInfo;
    }

    public Location locationOf(Catalog.Graph graph, Boolean requireWritable) {
        return locationMap.computeIfAbsent(
                graph,
                k -> catalogManager.locationOf(
                        transactionInfo.getSessionDatabaseReference(),
                        graph,
                        requireWritable,
                        transactionInfo.getRoutingContext()));
    }

    public int size() {
        return locationMap.size();
    }
}
