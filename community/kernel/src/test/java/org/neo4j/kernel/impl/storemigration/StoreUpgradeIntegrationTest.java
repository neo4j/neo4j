/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;

public class StoreUpgradeIntegrationTest
{
    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        HashMap params = new HashMap();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        GraphDatabaseService database = new EmbeddedGraphDatabase( workingDirectory.getPath(), params );
        database.shutdown();

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws IOException
    {
        File workingDirectory = new File(
                "target/" + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldAbordOnNonCleanlyShutdown" );
        prepareSampleLegacyDatabase( workingDirectory );

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );
        StoreUpgraderTestIT.truncateAllFiles(workingDirectory);
        // Now everything has lost the version info

        Map<String, String> params = new HashMap<String, String>();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        try
        {
            GraphDatabaseService database = new EmbeddedGraphDatabase(
                    workingDirectory.getPath(), params );
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
        File workingDirectory = new File(
                "target/" + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldAbortOnCorruptStore" );
        prepareSampleLegacyDatabase( workingDirectory );

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );
        truncateFile( new File( workingDirectory,
                "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore v0.9.9" );

        Map<String, String> params = new HashMap<String, String>();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        try
        {
            GraphDatabaseService database = new EmbeddedGraphDatabase(
                    workingDirectory.getPath(), params );
            fail( "Should have been unable to start upgrade on old version" );
        }
        catch ( RuntimeException e )
        {
            assertTrue( UnableToUpgradeException.class.isAssignableFrom( e.getCause().getCause().getCause().getClass() ) );
        }
    }
}
