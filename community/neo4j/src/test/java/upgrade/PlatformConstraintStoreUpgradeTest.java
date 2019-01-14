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
package upgrade;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;

@RunWith( Parameterized.class )
public class PlatformConstraintStoreUpgradeTest
{
    @Rule
    public final TestDirectory storeDir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private File prepareDir;
    private File workingDir;

    @Parameterized.Parameter( 0 )
    public String version;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return Collections.singletonList( StandardV2_3.STORE_VERSION );
    }

    @Before
    public void setup()
    {
        prepareDir = storeDir.directory( "prepare" );
        workingDir = storeDir.directory( "working" );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationTest() throws IOException
    {
        checkForStoreVersion( version );
    }

    private void checkForStoreVersion( String storeVersion ) throws IOException
    {
        MigrationTestUtils.prepareSampleLegacyDatabase( storeVersion, fileSystemRule.get(), workingDir, prepareDir );
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
                .setConfig( GraphDatabaseSettings.allow_upgrade, "true" )
                .setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME ).newGraphDatabase();
    }
}
