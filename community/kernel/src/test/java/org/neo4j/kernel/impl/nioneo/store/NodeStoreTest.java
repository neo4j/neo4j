/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.nioneo.store.NodeStore.readOwnerFromDynamicLabelsRecord;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

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
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

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
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 60 ) );

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
        allocateFromNumbers( ids, dynamicRecords.iterator(), new PreAllocatedRecords( 8 ) );

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
        Config config = new Config();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        WindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
        StoreFactory factory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory, fs, DEV_NULL, new DefaultTxHook() );
        File nodeStoreFileName = new File( "nodestore" );
        factory.createNodeStore( nodeStoreFileName );
        NodeStore nodeStore = factory.newNodeStore( nodeStoreFileName );

        // -- a record with the msb carrying a negative value
        long nodeId = 0, labels = 0x8000000001L;
        NodeRecord record = new NodeRecord( nodeId, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
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
        fs.shutdown();
    }

    @Test
    public void shouldKeepRecordLightWhenSettingLabelFieldWithoutDynamicRecords() throws Exception
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        record.setLabelField( 0, Collections.<DynamicRecord>emptyList() );

        // THEN
        assertTrue( record.isLight() );
    }

    @Test
    public void shouldMarkRecordHeavyWhenSettingLabelFieldWithDynamicRecords() throws Exception
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        DynamicRecord dynamicRecord = new DynamicRecord( 1 );
        record.setLabelField( 0x8000000001L, asList( dynamicRecord ) );

        // THEN
        assertFalse( record.isLight() );
    }
}
