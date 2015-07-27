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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertTrue;

public class StoreMigratorTest
{
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    private File createNeoStoreWithOlderVersion( String version ) throws IOException
    {
        File storeDir = directory.directory().getAbsoluteFile();
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDir );
        return storeDir;
    }

    @Test
    public void shouldBeAbleToResumeMigration() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = createNeoStoreWithOlderVersion( Legacy21Store.LEGACY_VERSION );
        // and a state of the migration saying that it has done the actual migration
        Logging logging = new DevNullLoggingService();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreMigrator migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs, logging );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        assertTrue( migrator.needsMigration( storeDirectory ) );
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider, pageCache );
        migrator.close();

        // WHEN simulating resuming the migration
        migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs, logging );
        migrator.moveMigratedFiles( migrationDir, storeDirectory );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory( fs, storeDirectory, pageCache,
                logging.getMessagesLog( getClass() ), new Monitors() );
        storeFactory.newNeoStore( false ).close();
    }

    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    public final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
}
