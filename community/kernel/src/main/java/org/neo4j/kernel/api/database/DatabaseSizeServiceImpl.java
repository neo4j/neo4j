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
package org.neo4j.kernel.api.database;

import static org.neo4j.io.fs.FileSystemUtils.size;

import java.io.IOException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

public class DatabaseSizeServiceImpl implements DatabaseSizeService {
    private final DatabaseManager<? extends DatabaseContext> databaseManager;

    public DatabaseSizeServiceImpl(DatabaseManager<? extends DatabaseContext> databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public long getDatabaseTotalSize(NamedDatabaseId databaseId) throws IOException {
        var database = getDatabase(databaseId);
        var fs = getFileSystem(database);
        return getTotalSize(fs, database.getDatabaseLayout());
    }

    @Override
    public long getDatabaseDataSize(NamedDatabaseId databaseId) throws IOException {
        var database = getDatabase(databaseId);
        var fs = getFileSystem(database);
        return getDataDirectorySize(fs, database.getDatabaseLayout());
    }

    private static FileSystemAbstraction getFileSystem(Database database) {
        return database.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);
    }

    private Database getDatabase(NamedDatabaseId databaseId) {
        return databaseManager
                .getDatabaseContext(databaseId)
                .orElseThrow(() -> new DatabaseNotFoundException("Database " + databaseId.name() + " not found."))
                .database();
    }

    private static long getTotalSize(FileSystemAbstraction fs, DatabaseLayout databaseLayout) throws IOException {
        long dataDirectorySize = getDataDirectorySize(fs, databaseLayout);
        if (databaseLayout.getTransactionLogsDirectory().equals(databaseLayout.databaseDirectory())) {
            return dataDirectorySize;
        }
        return dataDirectorySize + size(fs, databaseLayout.getTransactionLogsDirectory());
    }

    private static long getDataDirectorySize(FileSystemAbstraction fs, DatabaseLayout databaseLayout)
            throws IOException {
        return size(fs, databaseLayout.databaseDirectory());
    }
}
