/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;

public class PlatformConstraintBackupTest
{
    @Rule
    public TestDirectory storeDir = TestDirectory.testDirectory();

    private File workingDir;

    @Before
    public void setup()
    {
        workingDir = storeDir.directory( "working" );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfiguration()
    {
        try
        {
            GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( workingDir );
            builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
            builder.setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME );
            builder.newGraphDatabase();
            fail( "Should not have created database with custom IO configuration and online backup." );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( OnlineBackupKernelExtension.CUSTOM_IO_EXCEPTION_MESSAGE,
                    ex.getCause().getCause().getMessage() );
        }
    }

    @Test
    public void shouldNotFailToStartWithCustomIOConfigurationWhenOnlineBackupIsDisabled() throws Exception
    {
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( workingDir );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        builder.setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME );
        builder.newGraphDatabase().shutdown();
    }

    private GraphDatabaseService createGraphDatabaseService()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( workingDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME ).newGraphDatabase();
    }
}
