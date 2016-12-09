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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allLegacyStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveNoTrailer;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasDefaultFormatVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;

@RunWith( Parameterized.class )
public class StoreUpgradeOnStartupTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Parameterized.Parameter( 0 )
    public String version;

    private static final Set<String> STORES_WITHOUT_VERSIONS = new HashSet<>( asList( StandardV2_3.STORE_VERSION,
            StandardV3_0.STORE_VERSION ) );

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private File workingDirectory;
    private StoreVersionCheck check;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return asList(
                StandardV2_0.STORE_VERSION,
                StandardV2_1.STORE_VERSION,
                StandardV2_2.STORE_VERSION,
                StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION
        );
    }

    @Before
    public void setup() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        workingDirectory = testDir.directory( "working_" + version );
        check = new StoreVersionCheck( pageCache );
        File prepareDirectory = testDir.directory( "prepare_" + version );
        prepareSampleLegacyDatabase( version, fileSystem, workingDirectory, prepareDirectory );
        assertEquals( !STORES_WITHOUT_VERSIONS.contains( version ),
                allLegacyStoreFilesHaveVersion( fileSystem, workingDirectory, version ) );
    }

    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException, ConsistencyCheckIncompleteException
    {
        // when
        GraphDatabaseService database = createGraphDatabaseService();
        database.shutdown();

        // then
        assertTrue( "Some store files did not have the correct version",
                checkNeoStoreHasDefaultFormatVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fileSystem, workingDirectory ) );
        assertConsistentStore( workingDirectory );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws Throwable
    {
        // given
        makeDbNotCleanlyShutdown();
        try
        {
            // when
            GraphDatabaseService database = createGraphDatabaseService();
            database.shutdown();// shutdown db in case test fails
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            // then
            assertThat( Exceptions.rootCause( e ),
                    Matchers.instanceOf( StoreUpgrader.UnableToUpgradeException.class ) );
        }
    }

    private void makeDbNotCleanlyShutdown() throws IOException
    {
        if ( STORES_WITHOUT_VERSIONS.contains( version ) )
        {
            removeCheckPointFromTxLog( fileSystem, workingDirectory );
        }
        else
        {
            File file = new File( workingDirectory, "neostore.propertystore.db.index.keys" );
            truncateFile( fileSystem, file, "StringPropertyStore " + version );
        }
    }

    private GraphDatabaseService createGraphDatabaseService()
    {
        return new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( workingDirectory )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
                .newGraphDatabase();
    }
}
