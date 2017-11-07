/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;

public class RebuildReplicatedIdGeneratorsTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private ReplicatedIdRangeAcquirer idRangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );

    @Test
    public void rebuildReplicatedIdGeneratorsOnRecovery() throws Exception
    {
        DefaultFileSystemAbstraction fileSystem = fileSystemRule.get();
        File stickyGenerator = new File( testDirectory.graphDbDir(), "stickyGenerator" );
        File nodeStoreIdGenerator = new File( testDirectory.graphDbDir(), MetaDataStore.DEFAULT_NAME + NODE_STORE_NAME + ".id" );

        StoreFactory storeFactory = new StoreFactory( testDirectory.graphDbDir(), Config.defaults(),
                getIdGenerationFactory( fileSystem ), pageCacheRule.getPageCache( fileSystem ), fileSystem,
                NullLogProvider.getInstance() );
        try ( NeoStores neoStores = storeFactory.openAllNeoStores( true ) )
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            for ( int i = 0; i < 50; i++ )
            {
                NodeRecord nodeRecord = nodeStore.newRecord();
                nodeRecord.setInUse( true );
                nodeRecord.setId( nodeStore.nextId() );
                if ( i == 47 )
                {
                    FileUtils.copyFile( nodeStoreIdGenerator, stickyGenerator );
                }
                nodeStore.updateRecord( nodeRecord );
            }
            neoStores.close();
        }

        FileUtils.copyFile( stickyGenerator, nodeStoreIdGenerator );
        try ( NeoStores reopenedStores = storeFactory.openAllNeoStores() )
        {
            reopenedStores.makeStoreOk();
            assertEquals( 51L, reopenedStores.getNodeStore().nextId() );
        }
    }

    private ReplicatedIdGeneratorFactory getIdGenerationFactory( FileSystemAbstraction fileSystemAbstraction )
    {
        when( idRangeAcquirer.acquireIds( IdType.NODE ) ).thenReturn( new IdAllocation( new IdRange(
                PrimitiveLongCollections.EMPTY_LONG_ARRAY, 0, 10000 ), 0, 0 ) );
        return new ReplicatedIdGeneratorFactory( fileSystemAbstraction, idRangeAcquirer, NullLogProvider.getInstance(),
                new EnterpriseIdTypeConfigurationProvider( Config.defaults() ) );
    }
}
