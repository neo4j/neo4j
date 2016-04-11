/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package upgrade;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PlatformConstraintStoreUpgradeTest
{
    @Rule
    public TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldFailToStartIfConfiguredForCAPIDeviceTest()
    {
        GraphDatabaseBuilder gdBuilder =
                new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir.graphDbDir() );
        gdBuilder.setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.pagecache_swapper, "custom" );
        try
        {
            gdBuilder.newGraphDatabase();
            fail( "Should not have created database with custom IO configuration and Store Upgrade." );
        }
        catch ( RuntimeException ex )
        {
            // good
            assertEquals( "Store upgrade not allowed with custom IO integrations", ex.getMessage() );
        }
    }
}
