/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.highlimit;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.test.rule.PageCacheRule;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class HighLimitStoreMigrationTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private final FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

    @Test
    public void haveSameFormatCapabilitiesAsHighLimit3_0()
    {
        assertTrue( HighLimit.RECORD_FORMATS.hasSameCapabilities( HighLimitV3_0_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    public void doNotMigrateHighLimit3_0StoreFiles() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
        StoreMigrator migrator = new StoreMigrator( fileSystem, pageCache, Config.empty(), NullLogService.getInstance(),
                                                    schemaIndexProvider );

        File storeDir = new File( "storeDir" );
        File migrationDir = new File( "migrationDir" );
        fileSystem.mkdir( migrationDir );

        prepareNeoStoreFile( storeDir, HighLimitV3_0_0.STORE_VERSION, pageCache );

        MigrationProgressMonitor.Section progressMonitor = mock( MigrationProgressMonitor.Section.class );
        migrator.migrate( storeDir, migrationDir, progressMonitor, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

        File[] migrationFiles = fileSystem.listFiles( migrationDir );
        Set<String> fileNames = Stream.of( migrationFiles ).map( File::getName ).collect( Collectors.toSet() );
        assertThat( "Only specified files should be created after migration attempt from 3.0 to 3.1 using high limit " +
                    "format. Since formats are compatible and migration is not required.", fileNames,
                CoreMatchers.hasItems( "lastxinformation", "lastxlogposition" ) );
    }

    private File prepareNeoStoreFile( File storeDir, String storeVersion, PageCache pageCache ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile( storeDir );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
        return neoStoreFile;
    }

    private File createNeoStoreFile( File storeDir ) throws IOException
    {
        fileSystem.mkdir( storeDir );
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        fileSystem.create( neoStoreFile ).close();
        return neoStoreFile;
    }

}
