/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class HighLimitStoreMigrationTest
{
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public RuleChain chain = RuleChain.outerRule( pageCacheRule )
                                      .around( fileSystemRule )
                                      .around( testDirectory );

    @Test
    public void haveDifferentFormatCapabilitiesAsHighLimit3_0()
    {
        assertFalse( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_0_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    public void migrateHighLimit3_0StoreFiles() throws IOException
    {
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, Config.defaults(), NullLogService.getInstance() );

        File storeDir = new File( testDirectory.graphDbDir(), "storeDir" );
        File migrationDir = new File( testDirectory.graphDbDir(), "migrationDir" );
        fileSystem.mkdir( migrationDir );
        fileSystem.mkdir( storeDir );

        prepareNeoStoreFile( fileSystem, storeDir, HighLimitV3_0_0.STORE_VERSION, pageCache );

        ProgressReporter progressMonitor = mock( ProgressReporter.class );

        migrator.migrate( storeDir, migrationDir, progressMonitor, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

        int newStoreFilesCount = fileSystem.listFiles( migrationDir ).length;
        assertThat( "Store should be migrated and new store files should be created.",
                newStoreFilesCount, Matchers.greaterThanOrEqualTo( StoreType.values().length ) );
    }

    private File prepareNeoStoreFile( FileSystemAbstraction fileSystem, File storeDir, String storeVersion,
            PageCache pageCache ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile( fileSystem, storeDir );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
        return neoStoreFile;
    }

    private File createNeoStoreFile( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
    {
        fileSystem.mkdir( storeDir );
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        fileSystem.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
