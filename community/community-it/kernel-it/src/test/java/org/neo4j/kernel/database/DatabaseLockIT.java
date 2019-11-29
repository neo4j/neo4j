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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.Test;

import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@DbmsExtension
class DatabaseLockIT
{
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void allDatabasesLockedWhenStarted()
    {
        var databaseNames = managementService.listDatabases();
        assertThat( databaseNames ).isNotEmpty();

        for ( var databaseName : databaseNames )
        {
            var dbApi = (GraphDatabaseAPI) managementService.database( databaseName );
            assertThrows( FileLockException.class, () -> LockChecker.checkDatabaseLock( dbApi.databaseLayout() ) );
        }
    }

    @Test
    void allDatabaseLocksReleasedWhenStopped()
    {
        var databaseNames = managementService.listDatabases();
        assertThat( databaseNames ).isNotEmpty();

        for ( var databaseName : databaseNames )
        {
            var dbApi = (GraphDatabaseAPI) managementService.database( databaseName );
            var db = dbApi.getDependencyResolver().resolveDependency( Database.class );
            db.stop();

            assertDoesNotThrow( () -> LockChecker.checkDatabaseLock( dbApi.databaseLayout() ).close() );
        }
    }

    @Test
    void dbmsLockedWhenStarted()
    {
        var dbApi = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        var neo4jLayout = dbApi.databaseLayout().getNeo4jLayout();

        assertThrows( FileLockException.class, () -> LockChecker.checkDbmsLock( neo4jLayout ) );
    }

    @Test
    void dbmsLockReleasedWhenStopped()
    {
        var dbApi = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        var neo4jLayout = dbApi.databaseLayout().getNeo4jLayout();

        managementService.shutdown();

        assertDoesNotThrow( () -> LockChecker.checkDbmsLock( neo4jLayout ).close() );
    }
}
