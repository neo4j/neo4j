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

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorImpl;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@PageCacheExtension
class FreeIdsAfterRecoveryTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @Test
    void shouldCompletelyRebuildIdGeneratorsAfterCrash()
    {
        // GIVEN
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        StoreFactory storeFactory = new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                NullLogProvider.getInstance() );
        long highId;
        try ( NeoStores stores = storeFactory.openAllNeoStores( true ) )
        {
            // a node store with a "high" node
            NodeStore nodeStore = stores.getNodeStore();
            nodeStore.setHighId( 20 );
            nodeStore.updateRecord( node( nodeStore.nextId() ) );
            highId = nodeStore.getHighId();
        }

        // populating its .id file with a bunch of ids
        File nodeIdFile = databaseLayout.idNodeStore();
        try ( IdGeneratorImpl idGenerator = new IdGeneratorImpl( fileSystem, nodeIdFile, 10, 10_000, false, IdType.NODE, () -> highId ) )
        {
            for ( long id = 0; id < 15; id++ )
            {
                idGenerator.freeId( id );
            }

            // WHEN
            try ( NeoStores stores = storeFactory.openAllNeoStores( true ) )
            {
                NodeStore nodeStore = stores.getNodeStore();
                assertFalse( nodeStore.getStoreOk() );

                // simulating what recovery does
                nodeStore.deleteIdGenerator();
                // recovery happens here...
                nodeStore.makeStoreOk();

                // THEN
                assertEquals( highId, nodeStore.nextId() );
            }
        }
    }

    private static NodeRecord node( long nextId )
    {
        NodeRecord node = new NodeRecord( nextId );
        node.setInUse( true );
        return node;
    }
}
