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
 * Locker service implementation that provide dbms and database level locks that are globally
 * registered to prevent file channel locks to be missed when channel will be closed.
 * @see GlobalLocker
 */
public class GlobalLockerService implements FileLockerService
{
    @Override
    public Locker createStoreLocker( FileSystemAbstraction fileSystem, Neo4jLayout storeLayout )
    {
        return new GlobalLocker( fileSystem, storeLayout );
    }

    @Override
    public Locker createDatabaseLocker( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout )
    {
        return new DatabaseLocker( fileSystem, databaseLayout );
    }
}
