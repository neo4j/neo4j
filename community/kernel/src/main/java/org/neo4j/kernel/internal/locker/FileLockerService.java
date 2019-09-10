/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.internal.locker;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

/**
 * Service to provide DBMS and individual databases file lockers
 */
public interface FileLockerService
{
    /**
     * Create DBMS level file locker
     * @param fileSystem file system to lock DBMS file in
     * @param storeLayout dbms store layout
     * @return DBMS locker
     */
    Locker createStoreLocker( FileSystemAbstraction fileSystem, Neo4jLayout storeLayout );

    /**
     * Create database lever file locker
     * @param fileSystem file system to lock database file in
     * @param databaseLayout database layout
     * @return database locker
     */
    Locker createDatabaseLocker( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout );
}
