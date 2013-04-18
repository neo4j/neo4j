/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.LEGACY_VERSION;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.test.TargetDirectory;

public class StoreUpgradeIntegrationTest
{
    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException
    {
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        HashMap params = new HashMap();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(workingDirectory.getPath()).setConfig( params ).newGraphDatabase();
        database.shutdown();

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws IOException
    {
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );
        StoreUpgraderTestIT.truncateAllFiles( fileSystem, workingDirectory );
        // Now everything has lost the version info

        Map<String, String> params = new HashMap<String, String>();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        try
        {
            new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                    workingDirectory.getPath()).setConfig( params ).newGraphDatabase();
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            assertTrue( IllegalStateException.class.isAssignableFrom( e.getCause().getCause().getCause().getClass() ) );
        }
    }

    @Test
    public void shouldAbortOnCorruptStore() throws IOException
    {
        prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );
        truncateFile(fileSystem, new File( workingDirectory,
                "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore " + LEGACY_VERSION );

        Map<String, String> params = new HashMap<String, String>();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        try
        {
            GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(workingDirectory.getPath()).setConfig( params ).newGraphDatabase();
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            assertTrue( UnableToUpgradeException.class.isAssignableFrom( e.getCause().getCause().getCause().getClass() ) );
        }
    }
    
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private final File workingDirectory = TargetDirectory.forTest( getClass() ).graphDbDir( true );
}
