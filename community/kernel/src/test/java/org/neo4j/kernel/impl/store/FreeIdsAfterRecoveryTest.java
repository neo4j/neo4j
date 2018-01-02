/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.HEADER_SIZE;
import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.markAsSticky;

public class FreeIdsAfterRecoveryTest
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldCompletelyRebuildIdGeneratorsAfterCrash() throws Exception
    {
        // GIVEN
        StoreFactory storeFactory = new StoreFactory( fs, directory.directory(), pageCacheRule.getPageCache( fs ),
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
        File nodeIdFile = new File( directory.directory(), StoreFile.NODE_STORE.fileName( StoreFileType.ID ) );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fs, nodeIdFile, 10, 10_000, false, highId );
        for ( long id = 0; id < 15; id++ )
        {
            idGenerator.freeId( id );
        }
        idGenerator.close();
        // marking as sticky to simulate a crash
        try ( StoreChannel channel = fs.open( nodeIdFile, "rw" ) )
        {
            markAsSticky( channel, ByteBuffer.allocate( HEADER_SIZE ) );
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

    private NodeRecord node( long nextId )
    {
        NodeRecord node = new NodeRecord( nextId );
        node.setInUse( true );
        return node;
    }
}
