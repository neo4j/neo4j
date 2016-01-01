/**
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateAllFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store.LEGACY_VERSION;

public class StoreUpgradeOnStartupTest
{
    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException, ConsistencyCheckIncompleteException
    {
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        HashMap<String, String> params = new HashMap<>();
        params.put( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );

        GraphDatabaseService database = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( workingDirectory.getPath() ).setConfig( params ).newGraphDatabase();
        database.shutdown();

        assertTrue( "Some store files did not have the correct version",
                allStoreFilesHaveVersion( fileSystem, workingDirectory, ALL_STORES_VERSION ) );
        assertConsistentStore( workingDirectory );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws Throwable
    {
        // Given
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );
        truncateAllFiles( fileSystem, workingDirectory );
        // Now everything has lost the version info

        try
        {
            // When
            new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                    workingDirectory.getPath()).setConfig( upgradeConfig() ).newGraphDatabase();

            // Then
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            assertThat( Exceptions.rootCause( e ), Matchers.instanceOf(
                    StoreUpgrader.UpgradingStoreVersionNotFoundException.class ) );
        }
    }

    @Test
    public void shouldAbortOnCorruptStore() throws IOException
    {
        // Given
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );
        truncateFile(fileSystem, new File( workingDirectory,
                "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore " + LEGACY_VERSION );

        try
        {
            // When
            GraphDatabaseService database = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( workingDirectory.getPath() ).setConfig( upgradeConfig() ).newGraphDatabase();

            // Then
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            assertThat( Exceptions.rootCause( e ), Matchers.instanceOf( UnableToUpgradeException.class ) );
        }
    }

    private Map<String, String> upgradeConfig()
    {
        Map<String, String> params = new HashMap<>();
        params.put( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
        return params;
    }
    
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private final File workingDirectory = TargetDirectory.forTest( getClass() ).makeGraphDbDir();

}
