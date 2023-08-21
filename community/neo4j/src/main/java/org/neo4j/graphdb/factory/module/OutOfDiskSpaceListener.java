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
package org.neo4j.graphdb.factory.module;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;

import java.util.HashSet;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.monitoring.ExceptionalDatabaseEvent;
import org.neo4j.logging.InternalLog;

public class OutOfDiskSpaceListener extends DatabaseEventListenerAdapter {
    private final Config globalConfig;
    private final InternalLog log;

    public OutOfDiskSpaceListener(Config globalConfig, InternalLog log) {
        this.globalConfig = globalConfig;
        this.log = log;
    }

    @Override
    public void databaseOutOfDiskSpace(DatabaseEventContext event) {
        var databaseEvent = (ExceptionalDatabaseEvent) event;
        var databaseName = databaseEvent.getDatabaseName();
        if (!SYSTEM_DATABASE_NAME.equals(databaseName)) {
            makeReadOnly(databaseName);
        }
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
            // FIXME ODP If this is the recommended way of resetting the read-only state,
            //  we need to make it available in community edition as well
            log.error(String.format(
                    "As a result of the database failing to allocate enough disk space, it has been put into read-only mode to protect from system failure and ensure data integrity. "
                            + "Please free up more disk space before changing access mode for database back to read-write state. "
                            + "Making database writable again can be done by:%n"
                            + "    CALL dbms.listConfig(\"%s\") YIELD value%n"
                            + "    WITH value%n"
                            + "    CALL dbms.setConfigValue(\"%s\", replace(value, \"<databaseName>\", \"\"))",
                    read_only_databases.name(), read_only_databases.name()));
        }
    }
}
