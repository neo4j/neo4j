/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateAllFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;

@RunWith(Parameterized.class)
public class StoreUpgradeOnStartupTest
{
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    private final String version;
    private final File workingDirectory;

    public StoreUpgradeOnStartupTest( String version )
    {
        this.version = version;
        workingDirectory = TargetDirectory.forTest( getClass() ).cleanDirectory( version );
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{Legacy19Store.LEGACY_VERSION},
                new Object[]{Legacy20Store.LEGACY_VERSION},
                new Object[]{Legacy21Store.LEGACY_VERSION}
        );
    }

    @Before
    public void setup() throws IOException
    {
        prepareSampleLegacyDatabase( version, fileSystem, workingDirectory );
        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, version ) );
    }

    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException, ConsistencyCheckIncompleteException
    {
        // when
        GraphDatabaseService database = createGraphDatabaseService();
        database.shutdown();

        // then
        assertTrue( "Some store files did not have the correct version",
                allStoreFilesHaveVersion( fileSystem, workingDirectory, ALL_STORES_VERSION ) );
        assertConsistentStore( workingDirectory );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws Throwable
    {
        // given
        truncateAllFiles( fileSystem, workingDirectory, version );
        // Now everything has lost the version info

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
            assertThat( Exceptions.rootCause( e ), Matchers.instanceOf(
                    StoreUpgrader.UpgradingStoreVersionNotFoundException.class ) );
        }
    }

    @Test
    public void shouldAbortOnCorruptStore() throws IOException
    {
        // given
        File file = new File( workingDirectory, "neostore.propertystore.db.index.keys" );
        truncateFile( fileSystem, file, "StringPropertyStore " + version );

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
            assertThat( Exceptions.rootCause( e ), Matchers.instanceOf( UnableToUpgradeException.class ) );
        }
    }

    private GraphDatabaseService createGraphDatabaseService()
    {
        return new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( workingDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" )
                .newGraphDatabase();
    }
}
