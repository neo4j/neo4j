/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertTrue;

public class StoreMigratorTest
{
    private SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    private File createNeoStoreWithOlderVersion( String version ) throws IOException
    {
        File storeDir = new File( "dir" ).getAbsoluteFile();
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( storeDir );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fileSystem, storeDir );
        return storeDir;
    }

    @Test
    public void shouldBeAbleToResumeMigration() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = createNeoStoreWithOlderVersion( Legacy20Store.LEGACY_VERSION );
        // and a state of the migration saying that it has done the actual migration
        Logging logging = new DevNullLoggingService();
        StoreMigrator migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs.get(), logging );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.get().mkdirs( migrationDir );
        assertTrue( migrator.needsMigration( storeDirectory ) );
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider );
        migrator.close();

        // WHEN simulating resuming the migration
        migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs.get(), logging );
        migrator.moveMigratedFiles( migrationDir, storeDirectory );

        // THEN starting the new store should be successful
        LifeSupport life = new LifeSupport();
        PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs.get(), getClass().getSimpleName(), life );
        life.start();
        try
        {
            StoreFactory storeFactory = new StoreFactory( fs.get(), storeDirectory, pageCache,
                    logging.getMessagesLog( getClass() ), new Monitors() );
            storeFactory.newNeoStore( false, false ).close();
        }
        finally
        {
            life.shutdown();
        }
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
