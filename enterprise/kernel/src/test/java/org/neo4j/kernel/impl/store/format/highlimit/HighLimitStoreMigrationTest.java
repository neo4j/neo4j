/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
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
    public void migrateHighLimit3_0StoreFiles() throws Exception
    {
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, Config.defaults(), NullLogService.getInstance(), jobScheduler );

            DatabaseLayout databaseLayout = testDirectory.databaseLayout();
            DatabaseLayout migrationLayout = testDirectory.databaseLayout( "migration" );

            prepareNeoStoreFile( fileSystem, databaseLayout, HighLimitV3_0_0.STORE_VERSION, pageCache );

            ProgressReporter progressMonitor = mock( ProgressReporter.class );

            migrator.migrate( databaseLayout, migrationLayout, progressMonitor, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

            int newStoreFilesCount = fileSystem.listFiles( migrationLayout.databaseDirectory() ).length;
            assertThat( "Store should be migrated and new store files should be created.", newStoreFilesCount,
                    Matchers.greaterThanOrEqualTo( StoreType.values().length ) );
        }
    }

    private static void prepareNeoStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, String storeVersion, PageCache pageCache )
            throws IOException
    {
        File neoStoreFile = createNeoStoreFile( fileSystem, databaseLayout );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private static File createNeoStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        fileSystem.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
