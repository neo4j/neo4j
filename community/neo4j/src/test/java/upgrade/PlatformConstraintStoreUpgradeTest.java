/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;

public class PlatformConstraintStoreUpgradeTest
{
    @Rule
    public final TestDirectory storeDir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private File prepareDir;
    private File workingDir;

    @Before
    public void setup()
    {

        prepareDir = storeDir.directory( "prepare" );
        workingDir = storeDir.directory( "working" );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationTest20() throws IOException
    {
        String storeVersion = StandardV2_0.STORE_VERSION;
        checkForStoreVersion( storeVersion );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationTest21() throws IOException
    {
        String storeVersion = StandardV2_1.STORE_VERSION;
        checkForStoreVersion( storeVersion );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationTest22() throws IOException
    {
        String storeVersion = StandardV2_2.STORE_VERSION;
        checkForStoreVersion( storeVersion );
    }

    protected void checkForStoreVersion( String storeVersion ) throws IOException
    {
        prepareSampleLegacyDatabase( storeVersion, fileSystemRule.get(), workingDir, prepareDir );
        try
        {
            createGraphDatabaseService();
            fail( "Should not have created database with custom IO configuration and Store Upgrade." );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( StoreMigrator.CUSTOM_IO_EXCEPTION_MESSAGE, ex.getCause().getCause().getMessage() );
        }
    }

    private GraphDatabaseService createGraphDatabaseService()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( workingDir )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
                .setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME ).newGraphDatabase();
    }
}
