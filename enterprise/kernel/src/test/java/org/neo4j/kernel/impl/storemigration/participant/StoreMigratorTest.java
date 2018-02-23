/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Resource;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.logging.NullLog;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.StoreFile.NEO_STORE;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.STORE;

@ExtendWith( TestDirectoryExtension.class )
public class StoreMigratorTest
{
    @Resource
    public TestDirectory directory;

    @Test
    public void shouldNotDoActualStoreMigrationBetween3_0_5_and_next() throws Exception
    {
        // GIVEN a store in vE.H.0 format
        File storeDir = directory.directory();
        new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                // The format should be vE.H.0, HighLimit.NAME may point to a different version in future versions
                .setConfig( GraphDatabaseSettings.record_format, HighLimitV3_0_0.NAME )
                .newGraphDatabase()
                .shutdown();
        Config config = Config.defaults( pagecache_memory, "8m" );

        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              PageCache pageCache = new ConfiguringPageCacheFactory( fs, config, NULL,
                      PageCursorTracerSupplier.NULL, NullLog.getInstance(), EmptyVersionContextSupplier.EMPTY )
                     .getOrCreatePageCache() )
        {
            // For test code sanity
            String fromStoreVersion = StoreVersion.HIGH_LIMIT_V3_0_0.versionString();
            Result hasVersionResult = new StoreVersionCheck( pageCache ).hasVersion(
                    new File( storeDir, NEO_STORE.fileName( STORE ) ), fromStoreVersion );
            assertTrue( hasVersionResult.outcome.isSuccessful(), hasVersionResult.actualVersion );

            // WHEN
            StoreMigrator migrator = new StoreMigrator( fs, pageCache, config, NullLogService.getInstance()
            );
            ProgressReporter monitor = mock( ProgressReporter.class );
            File migrationDir = new File( storeDir, "migration" );
            fs.mkdirs( migrationDir );
            migrator.migrate( storeDir, migrationDir, monitor, fromStoreVersion,
                    StoreVersion.HIGH_LIMIT_V3_0_6.versionString() );

            // THEN
            verifyNoMoreInteractions( monitor );
        }
    }

    @Test
    public void detectObsoleteCountStoresToRebuildDuringMigration()
    {
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        PageCache pageCache = mock( PageCache.class );
        Config config = Config.defaults();
        CountsMigrator storeMigrator = new CountsMigrator( fileSystem, pageCache, config );
        Set<String> actualVersions = new TreeSet<>();
        Set<String> expectedVersions = new TreeSet<>(
                Arrays.stream( StoreVersion.values() ).map( StoreVersion::versionString )
                        .collect( Collectors.toSet() ) );

        assertTrue( storeMigrator.countStoreRebuildRequired( StoreVersion.STANDARD_V2_3.versionString() ) );
        actualVersions.add( StoreVersion.STANDARD_V2_3.versionString() );
        assertTrue( storeMigrator.countStoreRebuildRequired( StoreVersion.STANDARD_V3_0.versionString() ) );
        actualVersions.add( StoreVersion.STANDARD_V3_0.versionString() );
        assertFalse( storeMigrator.countStoreRebuildRequired( StoreVersion.STANDARD_V3_2.versionString() ) );
        actualVersions.add( StoreVersion.STANDARD_V3_2.versionString() );
        assertFalse( storeMigrator.countStoreRebuildRequired( StoreVersion.STANDARD_V3_4.versionString() ) );
        actualVersions.add( StoreVersion.STANDARD_V3_4.versionString() );

        assertTrue( storeMigrator.countStoreRebuildRequired( StoreVersion.HIGH_LIMIT_V3_0_0.versionString() ) );
        actualVersions.add( StoreVersion.HIGH_LIMIT_V3_0_0.versionString() );
        assertTrue( storeMigrator.countStoreRebuildRequired( StoreVersion.HIGH_LIMIT_V3_0_6.versionString() ) );
        actualVersions.add( StoreVersion.HIGH_LIMIT_V3_0_6.versionString() );
        assertTrue( storeMigrator.countStoreRebuildRequired( StoreVersion.HIGH_LIMIT_V3_1_0.versionString() ) );
        actualVersions.add( StoreVersion.HIGH_LIMIT_V3_1_0.versionString() );
        assertFalse( storeMigrator.countStoreRebuildRequired( StoreVersion.HIGH_LIMIT_V3_2_0.versionString() ) );
        actualVersions.add( StoreVersion.HIGH_LIMIT_V3_2_0.versionString() );
        assertFalse( storeMigrator.countStoreRebuildRequired( StoreVersion.HIGH_LIMIT_V3_4_0.versionString() ) );
        actualVersions.add( StoreVersion.HIGH_LIMIT_V3_4_0.versionString() );

        assertEquals( expectedVersions, actualVersions );
    }

}
