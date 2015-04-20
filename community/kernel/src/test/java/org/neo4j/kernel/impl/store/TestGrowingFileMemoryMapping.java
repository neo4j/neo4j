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
package org.neo4j.kernel.impl.store;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.CommonAbstractStore.Configuration;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.Settings.osIsWindows;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestGrowingFileMemoryMapping
{
    private static final int MEGA = 1024 * 1024;

    @Test
    public void shouldGrowAFileWhileContinuingToMemoryMapNewRegions() throws Exception
    {
        // don't run on windows because memory mapping doesn't work properly there
        assumeTrue( !osIsWindows() );

        // given
        int NUMBER_OF_RECORDS = 1000000;

        File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
        Config config = new Config( stringMap(
                pagecache_memory.name(), mmapSize( NUMBER_OF_RECORDS, NodeStore.RECORD_SIZE ),
                Configuration.store_dir.name(), storeDir.getPath() ), NodeStore.Configuration.class );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        Monitors monitors = new Monitors();
        DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction, config );
        StoreFactory storeFactory = new StoreFactory(
                config,
                idGeneratorFactory,
                pageCache,
                fileSystemAbstraction,
                StringLogger.DEV_NULL,
                monitors );

        File fileName = new File( storeDir, NeoStore.DEFAULT_NAME + ".nodestore.db" );
        storeFactory.createEmptyStore( fileName, storeFactory.buildTypeDescriptorAndVersion(
                NodeStore.TYPE_DESCRIPTOR ) );

        NodeStore nodeStore = new NodeStore(
                fileName,
                config,
                idGeneratorFactory,
                pageCache,
                fileSystemAbstraction,
                StringLogger.DEV_NULL,
                null,
                StoreVersionMismatchHandler.FORCE_CURRENT_VERSION,
                monitors );

        // when
        int iterations = 2 * NUMBER_OF_RECORDS;
        long startingId = nodeStore.nextId();
        long nodeId = startingId;
        for ( int i = 0; i < iterations; i++ )
        {
            NodeRecord record = new NodeRecord( nodeId, false, i, 0 );
            record.setInUse( true );
            nodeStore.updateRecord( record );
            nodeId = nodeStore.nextId();
        }

        // then
        NodeRecord record = new NodeRecord( 0, false, 0, 0 );
        for ( int i = 0; i < iterations; i++ )
        {
            record.setId( startingId + i );
            nodeStore.getRecord( i, record );
            assertTrue( "record[" + i + "] should be in use", record.inUse() );
            assertThat( "record[" + i + "] should have nextRelId of " + i,
                    record.getNextRel(), is( (long) i ) );
        }

        nodeStore.close();
    }

    private String mmapSize( int numberOfRecords, int recordSize )
    {
        int bytes = numberOfRecords * recordSize;
        if ( bytes < MEGA )
        {
            throw new IllegalArgumentException( "too few records: " + numberOfRecords );
        }
        return bytes / MEGA + "M";
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
}
