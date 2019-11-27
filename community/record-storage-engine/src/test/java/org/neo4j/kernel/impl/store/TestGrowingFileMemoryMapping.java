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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@PageCacheExtension
@Neo4jLayoutExtension
class TestGrowingFileMemoryMapping
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldGrowAFileWhileContinuingToMemoryMapNewRegions()
    {
        // given
        final int NUMBER_OF_RECORDS = 1000000;

        Config config = Config.defaults();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( testDirectory.getFileSystem(), immediate() );
        StoreFactory storeFactory = new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache,
                testDirectory.getFileSystem(), NullLogProvider.getInstance() );

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
            assertTrue( record.inUse(), "record[" + i + "] should be in use" );
            assertThat( record.getNextRel() ).as( "record[" + i + "] should have nextRelId of " + i ).isEqualTo( (long) i );
        }

        neoStores.close();
    }
}
