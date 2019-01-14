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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FreeIdsAfterRecoveryTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory ).around( fileSystemRule )
            .around( pageCacheRule );

    @Test
    public void shouldCompletelyRebuildIdGeneratorsAfterCrash()
    {
        // GIVEN
        StoreFactory storeFactory = new StoreFactory(
                directory.directory(), Config.defaults(), new DefaultIdGeneratorFactory( fileSystemRule.get() ),
                pageCacheRule.getPageCache( fileSystemRule.get() ), fileSystemRule.get(),
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
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
        File nodeIdFile = new File( directory.directory(), StoreFile.NODE_STORE.fileName( StoreFileType.ID ) );
        try ( IdGeneratorImpl idGenerator = new IdGeneratorImpl( fileSystemRule.get(), nodeIdFile, 10, 10_000, false, IdType.NODE, () -> highId ) )
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

    private NodeRecord node( long nextId )
    {
        NodeRecord node = new NodeRecord( nextId );
        node.setInUse( true );
        return node;
    }
}
