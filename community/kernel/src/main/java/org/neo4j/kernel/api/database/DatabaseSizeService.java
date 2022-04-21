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

import java.io.IOException;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Service to evaluate databases sizes
 */
public interface DatabaseSizeService {
    /**
     * Calculate total size of all files that are related to requested database. Includes all database files and transaction logs
     * @param databaseId id of the database to evaluate size
     * @return total size of the database
     * @throws IOException on exception during evaluating underlying files size
     */
    long getDatabaseTotalSize(NamedDatabaseId databaseId) throws IOException;

    /**
     * Calculate size of data files that are related to requested database. Includes database files but not transaction logs
     * @param databaseId id of the database to evaluate size
     * @return size of the database files
     * @throws IOException on exception during evaluating underlying files size
     */
    long getDatabaseDataSize(NamedDatabaseId databaseId) throws IOException;
}
