/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@PageCacheExtension
@Neo4jLayoutExtension
class NodeImporterTest
{
    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout layout;

    @Test
    void shouldHandleLargeAmountsOfLabels() throws IOException
    {
        // given
        IdMapper idMapper = mock( IdMapper.class );
        JobScheduler scheduler = new ThreadPoolJobScheduler();
        try ( Lifespan life = new Lifespan( scheduler );
              BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fs, pageCache, NULL, layout,
                      Standard.LATEST_RECORD_FORMATS, Configuration.DEFAULT, NullLogService.getInstance(), AdditionalInitialIds.EMPTY, Config.defaults(),
                      INSTANCE ) )
        {
            stores.createNew();

            // when
            int numberOfLabels = 50;
            long nodeId = 0;
            try ( NodeImporter importer = new NodeImporter( stores, idMapper, new DataImporter.Monitor(), NULL, INSTANCE ) )
            {
                importer.id( nodeId );
                String[] labels = new String[numberOfLabels];
                for ( int i = 0; i < labels.length; i++ )
                {
                    labels[i] = "Label" + i;
                }
                importer.labels( labels );
                importer.endOfEntity();
            }

            // then
            NodeStore nodeStore = stores.getNodeStore();
            NodeRecord record = nodeStore.getRecord( nodeId, nodeStore.newRecord(), RecordLoad.NORMAL, PageCursorTracer.NULL );
            long[] labels = NodeLabelsField.parseLabelsField( record ).get( nodeStore, PageCursorTracer.NULL );
            assertEquals( numberOfLabels, labels.length );
        }
    }

    @Test
    void tracePageCacheAccessOnNodeImport() throws IOException
    {
        JobScheduler scheduler = new ThreadPoolJobScheduler();
        try ( Lifespan life = new Lifespan( scheduler );
                BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fs, pageCache, NULL, layout,
                        Standard.LATEST_RECORD_FORMATS, Configuration.DEFAULT, NullLogService.getInstance(), AdditionalInitialIds.EMPTY, Config.defaults(),
                        INSTANCE ) )
        {
            stores.createNew();

            int numberOfLabels = 50;
            long nodeId = 0;
            var cacheTracer = new DefaultPageCacheTracer();
            try ( NodeImporter importer = new NodeImporter( stores, IdMappers.actual(), new DataImporter.Monitor(), cacheTracer, INSTANCE ) )
            {
                importer.id( nodeId );
                String[] labels = new String[numberOfLabels];
                for ( int i = 0; i < labels.length; i++ )
                {
                    labels[i] = "Label" + i;
                }
                importer.labels( labels );
                importer.property( "a", randomAscii( 10 ) );
                importer.property( "b", randomAscii( 100 ) );
                importer.property( "c", randomAscii( 1000 ) );
                importer.endOfEntity();
            }

            NodeStore nodeStore = stores.getNodeStore();
            NodeRecord record = nodeStore.getRecord( nodeId, nodeStore.newRecord(), RecordLoad.NORMAL, PageCursorTracer.NULL );
            long[] labels = NodeLabelsField.parseLabelsField( record ).get( nodeStore, PageCursorTracer.NULL );
            assertEquals( numberOfLabels, labels.length );

            assertThat( cacheTracer.faults() ).isEqualTo( 2 );
            assertThat( cacheTracer.pins() ).isEqualTo( 13 );
            assertThat( cacheTracer.unpins() ).isEqualTo( 13 );
            assertThat( cacheTracer.hits() ).isEqualTo( 11 );
        }
    }
}
