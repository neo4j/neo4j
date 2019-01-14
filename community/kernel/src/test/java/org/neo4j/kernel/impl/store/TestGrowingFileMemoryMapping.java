/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.ConfigurablePageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class TestGrowingFileMemoryMapping
{

    private final ConfigurablePageCacheRule pageCacheRule = new ConfigurablePageCacheRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule ).around( pageCacheRule );

    @Test
    public void shouldGrowAFileWhileContinuingToMemoryMapNewRegions()
    {
        // don't run on windows because memory mapping doesn't work properly there
        assumeTrue( !SystemUtils.IS_OS_WINDOWS );

        // given
        final int NUMBER_OF_RECORDS = 1000000;

        File storeDir = testDirectory.graphDbDir();
        Config config = Config.defaults( pagecache_memory, mmapSize( NUMBER_OF_RECORDS, NodeRecordFormat.RECORD_SIZE ) );
        FileSystemAbstraction fileSystemAbstraction = fileSystemRule.get();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystemAbstraction );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction, config );
        StoreFactory storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory, pageCache,
                fileSystemAbstraction, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );

        NeoStores neoStores = storeFactory.openAllNeoStores( true );
        NodeStore nodeStore = neoStores.getNodeStore();

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
            nodeStore.getRecord( i, record, NORMAL );
            assertTrue( "record[" + i + "] should be in use", record.inUse() );
            assertThat( "record[" + i + "] should have nextRelId of " + i,
                    record.getNextRel(), is( (long) i ) );
        }

        neoStores.close();
    }

    private String mmapSize( int numberOfRecords, int recordSize )
    {
        int bytes = numberOfRecords * recordSize;
        long mebiByte = ByteUnit.mebiBytes( 1 );
        if ( bytes < mebiByte )
        {
            throw new IllegalArgumentException( "too few records: " + numberOfRecords );
        }
        return bytes / mebiByte + "M";
    }
}
