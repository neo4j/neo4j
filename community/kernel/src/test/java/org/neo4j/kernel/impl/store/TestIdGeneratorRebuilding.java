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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class TestIdGeneratorRebuilding
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private EphemeralFileSystemAbstraction fs;
    private File storeDir;

    @Before
    public void doBefore()
    {
        fs = fsRule.get();
        storeDir = AbstractNeo4jTestCase.getStorePath( "xatest" );
        fs.mkdirs( storeDir );
    }

    private File file( String name )
    {
        return new File( storeDir, name );
    }

    @Test
    public void verifyFixedSizeStoresCanRebuildIdGeneratorSlowly() throws IOException
    {
        // Given we have a store ...
        Config config = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.rebuild_idgenerators_fast.name(), "false" ) );
        File storeFile = file( "nodes" );

        DynamicArrayStore labelStore = mock( DynamicArrayStore.class );
        NodeStore store = new NodeStore( storeFile, config, new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), NullLogProvider.getInstance(), labelStore );
        store.initialise( true );
        store.makeStoreOk();

        // ... that contain a number of records ...
        NodeRecord record = new NodeRecord( 0 );
        record.setInUse( true );
        int highestId = 50;
        for ( int i = 0; i < highestId; i++ )
        {
            assertThat( store.nextId(), is( (long) i ) );
            record.setId( i );
            store.updateRecord( record );
        }
        store.setHighestPossibleIdInUse( highestId );

        // ... and some have been deleted
        Long[] idsToFree = {2L, 3L, 5L, 7L};
        record.setInUse( false );
        for ( long toDelete : idsToFree )
        {
            record.setId( toDelete );
            store.updateRecord( record );
        }

        // Then when we rebuild the id generator
        store.rebuildIdGenerator();
        store.closeIdGenerator();
        store.openIdGenerator(); // simulate a restart to allow id reuse

        // We should observe that the ids above got freed
        List<Long> nextIds = new ArrayList<>();
        nextIds.add( store.nextId() ); // 2
        nextIds.add( store.nextId() ); // 3
        nextIds.add( store.nextId() ); // 5
        nextIds.add( store.nextId() ); // 7
        nextIds.add( store.nextId() ); // 51
        assertThat( nextIds, contains( 2L, 3L, 5L, 7L, 50L ) );
        store.close();
    }

    @Test
    public void verifyDynamicSizedStoresCanRebuildIdGeneratorSlowly() throws Exception
    {
        // Given we have a store ...
        Config config = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.rebuild_idgenerators_fast.name(), "false" ) );

        StoreFactory storeFactory = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), fs, NullLogProvider.getInstance() );
        NeoStores neoStores = storeFactory.openAllNeoStores( true );
        DynamicStringStore store = neoStores.getPropertyStore().getStringStore();

        // ... that contain a number of records ...
        DynamicRecord record = new DynamicRecord( 1 );
        record.setInUse( true, PropertyType.STRING.intValue() );
        int highestId = 50;
        for ( int i = 1; i <= highestId; i++ ) // id '0' is the dynamic store header
        {
            assertThat( store.nextId(), is( (long) i ) );
            record.setId( i );
            StringBuilder sb = new StringBuilder( i );
            for ( int j = 0; j < i; j++ )
            {
                sb.append( 'a' );
            }
            record.setData( sb.toString().getBytes( "UTF-16" ) );
            store.updateRecord( record );
        }
        store.setHighestPossibleIdInUse( highestId );

        // ... and some have been deleted
        Long[] idsToFree = {2L, 3L, 5L, 7L};
        record.setInUse( false );
        for ( long toDelete : idsToFree )
        {
            record.setId( toDelete );
            store.updateRecord( record );
        }

        // Then when we rebuild the id generator
        store.rebuildIdGenerator();

        // We should observe that the ids above got freed
        List<Long> nextIds = new ArrayList<>();
        nextIds.add( store.nextId() ); // 2
        nextIds.add( store.nextId() ); // 3
        nextIds.add( store.nextId() ); // 5
        nextIds.add( store.nextId() ); // 7
        nextIds.add( store.nextId() ); // 51
        assertThat( nextIds, contains( 2L, 3L, 5L, 7L, 51L ) );
        neoStores.close();
    }

    @Test
    public void rebuildingIdGeneratorMustNotMissOutOnFreeRecordsAtEndOfFilePage() throws IOException
    {
        // Given we have a store ...
        Config config = new Config( MapUtil.stringMap(
                GraphDatabaseSettings.rebuild_idgenerators_fast.name(), "false" ) );
        File storeFile = file( "nodes" );

        DynamicArrayStore labelStore = mock( DynamicArrayStore.class );
        NodeStore store = new NodeStore( storeFile, config, new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), NullLogProvider.getInstance(), labelStore );
        store.initialise( true );
        store.makeStoreOk();

        // ... that contain enough records to fill several file pages ...
        int recordsPerPage = store.getRecordsPerPage();
        NodeRecord record = new NodeRecord( 0 );
        record.setInUse( true );
        int highestId = recordsPerPage * 3; // 3 pages worth of records
        for ( int i = 0; i < highestId; i++ )
        {
            assertThat( store.nextId(), is( (long) i ) );
            record.setId( i );
            store.updateRecord( record );
        }
        store.setHighestPossibleIdInUse( highestId );

        // ... and some records at the end of a page have been deleted
        Long[] idsToFree = {recordsPerPage - 2L, recordsPerPage - 1L}; // id's are zero based, hence -2 and -1
        record.setInUse( false );
        for ( long toDelete : idsToFree )
        {
            record.setId( toDelete );
            store.updateRecord( record );
        }

        // Then when we rebuild the id generator
        store.rebuildIdGenerator();
        store.closeIdGenerator();
        store.openIdGenerator(); // simulate a restart to allow id reuse

        // We should observe that the ids above got freed
        List<Long> nextIds = new ArrayList<>();
        nextIds.add( store.nextId() ); // recordsPerPage - 2
        nextIds.add( store.nextId() ); // recordsPerPage - 1
        nextIds.add( store.nextId() ); // recordsPerPage * 3 (we didn't use this id in the create-look above)
        assertThat( nextIds, contains( recordsPerPage - 2L, recordsPerPage - 1L, recordsPerPage * 3L ) );
        store.close();
    }
}
