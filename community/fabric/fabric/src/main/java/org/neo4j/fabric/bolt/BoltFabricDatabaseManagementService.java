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
package org.neo4j.fabric.bolt;

import static java.lang.String.format;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.memory.MemoryTracker;

public class BoltFabricDatabaseManagementService implements BoltGraphDatabaseManagementServiceSPI {
    private final FabricExecutor fabricExecutor;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final FabricDatabaseManager fabricDatabaseManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;

    public BoltFabricDatabaseManagementService(
            FabricExecutor fabricExecutor,
            FabricConfig config,
            TransactionManager transactionManager,
            FabricDatabaseManager fabricDatabaseManager,
            LocalGraphTransactionIdTracker transactionIdTracker) {
        this.fabricExecutor = fabricExecutor;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricDatabaseManager = fabricDatabaseManager;
        this.transactionIdTracker = transactionIdTracker;
    }

    @Override
    public BoltGraphDatabaseServiceSPI database(String databaseName, MemoryTracker memoryTracker)
            throws UnavailableException, DatabaseNotFoundException {
        try {
            return getDatabase(databaseName, memoryTracker);
        } catch (DatabaseShutdownException | UnavailableException e) {
            // The failure expected over bolt
            throw new UnavailableException(format("Database '%s' is unavailable.", databaseName));
        }
    }

    public BoltGraphDatabaseServiceSPI getDatabase(String databaseName, MemoryTracker memoryTracker)
            throws UnavailableException, DatabaseNotFoundException {
        memoryTracker.allocateHeap(BoltFabricDatabaseService.SHALLOW_SIZE);

        var databaseReference = fabricDatabaseManager.getDatabaseReference(databaseName);
        return new BoltFabricDatabaseService(
                databaseReference, fabricExecutor, config, transactionManager, transactionIdTracker, memoryTracker);
    }
}
