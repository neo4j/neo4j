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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@ExtendWith( TestDirectoryExtension.class )
class NodeImporterTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldHandleLargeAmountsOfLabels() throws IOException
    {
        // given
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        FileSystemAbstraction fs = directory.getFileSystem();
        swapperFactory.open( fs, null );
        IdMapper idMapper = mock( IdMapper.class );
        JobScheduler scheduler = JobSchedulerFactory.createScheduler();
        try ( Lifespan life = new Lifespan( scheduler );
              PageCache pageCache = new MuninnPageCache( swapperFactory, 1_000, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                      EmptyVersionContextSupplier.EMPTY, scheduler );
              BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fs, pageCache, PageCacheTracer.NULL, directory.storeDir(),
                      Standard.LATEST_RECORD_FORMATS, Configuration.DEFAULT, NullLogService.getInstance(), AdditionalInitialIds.EMPTY, Config.defaults() ) )
        {
            stores.createNew();

            // when
            int numberOfLabels = 50;
            long nodeId = 0;
            try ( NodeImporter importer = new NodeImporter( stores, idMapper, new DataImporter.Monitor() ) )
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
            NodeRecord record = nodeStore.getRecord( nodeId, nodeStore.newRecord(), RecordLoad.NORMAL );
            long[] labels = NodeLabelsField.parseLabelsField( record ).get( nodeStore );
            assertEquals( numberOfLabels, labels.length );
        }
    }
}
