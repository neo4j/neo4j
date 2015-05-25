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

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.NodeStore.readOwnerFromDynamicLabelsRecord;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class NodeStoreTest
{
    @Test
    public void shouldReadFirstFromSingleRecordDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = 12l;
        long[] ids = new long[] { expectedId, 23l, 42l };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord );
        allocateFromNumbers( new ArrayList<DynamicRecord>(), ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    public void shouldReadFirstAsNullFromEmptyDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = null;
        long[] ids = new long[] { };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord );
        allocateFromNumbers( new ArrayList<DynamicRecord>(), ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    public void shouldReadFirstFromTwoRecordDynamicLongArray() throws Exception
    {
        // GIVEN
        Long expectedId = 12l;
        long[] ids = new long[] { expectedId, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l };
        DynamicRecord firstRecord = new DynamicRecord( 0l );
        List<DynamicRecord> dynamicRecords = asList( firstRecord, new DynamicRecord( 1l ) );
        allocateFromNumbers( new ArrayList<DynamicRecord>(), ids, dynamicRecords.iterator(), new PreAllocatedRecords( 8 ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    public void shouldCombineProperFiveByteLabelField() throws Exception
    {
        // GIVEN
        // -- a store
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        NodeStore nodeStore = newNodeStore( fs );

        // -- a record with the msb carrying a negative value
        long nodeId = 0, labels = 0x8000000001L;
        NodeRecord record = new NodeRecord( nodeId, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
        record.setInUse( true );
        record.setLabelField( labels, Collections.<DynamicRecord>emptyList() );
        nodeStore.updateRecord( record );

        // WHEN
        // -- reading that record back
        NodeRecord readRecord = nodeStore.getRecord( nodeId );

        // THEN
        // -- the label field must be the same
        assertEquals( labels, readRecord.getLabelField() );

        // CLEANUP
        nodeStore.close();
        fs.close();
    }

    @Test
    public void shouldKeepRecordLightWhenSettingLabelFieldWithoutDynamicRecords() throws Exception
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        record.setLabelField( 0, Collections.<DynamicRecord>emptyList() );

        // THEN
        assertTrue( record.isLight() );
    }

    @Test
    public void shouldMarkRecordHeavyWhenSettingLabelFieldWithDynamicRecords() throws Exception
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        DynamicRecord dynamicRecord = new DynamicRecord( 1 );
        record.setLabelField( 0x8000000001L, asList( dynamicRecord ) );

        // THEN
        assertFalse( record.isLight() );
    }

    @Test
    public void shouldTellNodeInUse() throws Exception
    {
        // Given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        NodeStore store = newNodeStore( fs );

        long exists = store.nextId();
        store.updateRecord( new NodeRecord( exists, false, 10, 20, true ) );

        long deleted = store.nextId();
        store.updateRecord( new NodeRecord( deleted, false, 10, 20, true ) );
        store.updateRecord( new NodeRecord( deleted, false, 10, 20, false ) );

        // When & then
        assertTrue( store.inUse( exists ) );
        assertFalse( store.inUse( deleted ) );
        assertFalse(store.inUse( IdType.NODE.getMaxValue() ));
    }

    @Test
    public void scanningRecordsShouldVisitEachInUseRecordOnce() throws IOException
    {
        // GIVEN we have a NodeStore with data that spans several pages...
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        NodeStore store = newNodeStore( fs );

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        final PrimitiveLongSet nextRelSet = Primitive.longSet();
        for ( int i = 0; i < 10_000; i++ )
        {
            // Enough records to span several pages
            int nextRelCandidate = rng.nextInt( 0, Integer.MAX_VALUE );
            if ( nextRelSet.add( nextRelCandidate ) )
            {
                long nodeId = store.nextId();
                NodeRecord record = new NodeRecord(
                        nodeId, false, nextRelCandidate, 20, true );
                store.updateRecord( record );
                if ( rng.nextInt( 0, 10 ) < 3 )
                {
                    nextRelSet.remove( nextRelCandidate );
                    record.setInUse( false );
                    store.updateRecord( record );
                }
            }
        }

        // ...WHEN we now have an interesting set of node records, and we
        // visit each and remove that node from our nextRelSet...

        Visitor<NodeRecord, IOException> scanner = new Visitor<NodeRecord, IOException>()
        {
            @Override
            public boolean visit( NodeRecord record ) throws IOException
            {
                // ...THEN we should observe that no nextRel is ever removed twice...
                assertTrue( nextRelSet.remove( record.getNextRel() ) );
                return false;
            }
        };
        store.scanAllRecords( scanner );

        // ...NOR do we have anything left in the set afterwards.
        assertTrue( nextRelSet.isEmpty() );
    }

    private NodeStore newNodeStore( EphemeralFileSystemAbstraction fs )
    {
        File storeDir = new File( "dir" );
        fs.mkdirs( storeDir );
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        Monitors monitors = new Monitors();
        StoreFactory factory = new StoreFactory(
                storeDir,
                new Config(),
                idGeneratorFactory,
                pageCacheRule.getPageCache( fs ),
                fs,
                NullLogProvider.getInstance(),
                monitors );
        factory.createNodeStore();
        nodeStore = factory.newNodeStore();
        return nodeStore;
    }

    private NodeStore nodeStore;

    @After
    public void tearDown()
    {
        if ( nodeStore != null )
        {
            nodeStore.close();
            nodeStore = null;
        }
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
}
