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
package org.neo4j.kernel.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( TestDirectoryExtension.class )
class StoreLockerLifecycleAdapterTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldAllowDatabasesToUseFilesetsSequentially()
    {
        DatabaseManagementService managementService = newDb();
        managementService.shutdown();
        managementService = newDb();
        managementService.shutdown();
    }

    @Test
    void shouldNotAllowDatabasesToUseFilesetsConcurrently()
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently( stringMap() );
    }

    @Test
    void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode()
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently(
                stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
    }

    private void shouldNotAllowDatabasesToUseFilesetsConcurrently( Map<String,String> config )
    {
        DatabaseManagementService managementService = newDb();
        DatabaseManagementService embeddedService = null;
        try
        {
            embeddedService = new TestDatabaseManagementServiceBuilder( directory.storeDir() ).setConfigRaw( config ).build();
            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause().getCause(), instanceOf( StoreLockException.class ) );
        }
        finally
        {
            if ( embeddedService != null )
            {
                embeddedService.shutdown();
            }
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
    }

    private DatabaseManagementService newDb()
    {
        return new TestDatabaseManagementServiceBuilder( directory.storeDir() ).build();
    }
}
