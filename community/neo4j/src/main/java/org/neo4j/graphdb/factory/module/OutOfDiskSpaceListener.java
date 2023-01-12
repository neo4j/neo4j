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
package org.neo4j.graphdb.factory.module;

import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;

import java.util.HashSet;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.monitoring.DefaultDatabaseEvent;

public class OutOfDiskSpaceListener extends DatabaseEventListenerAdapter {
    private final Config globalConfig;

    public OutOfDiskSpaceListener(Config globalConfig) {
        this.globalConfig = globalConfig;
    }

    @Override
    public void databaseOutOfDiskSpace(DatabaseEventContext event) {
        var databaseEvent = (DefaultDatabaseEvent) event;
        var databaseName = databaseEvent.getDatabaseName();
        makeReadOnly(databaseName);
    }

    /**
     * Synchronized to only let one thread at the time mark database as read only
     * @param databaseName the database to make read only
     */
    private synchronized void makeReadOnly(String databaseName) {
        var readOnlyDatabases = new HashSet<>(globalConfig.get(read_only_databases));
        if (readOnlyDatabases.add(databaseName)) {
            globalConfig.setDynamic(
                    read_only_databases,
                    readOnlyDatabases,
                    "Dynamic failover to read-only mode because of failure to allocate disk space.");
        }
    }
}
