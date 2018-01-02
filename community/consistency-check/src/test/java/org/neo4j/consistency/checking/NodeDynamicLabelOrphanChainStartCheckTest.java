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
package org.neo4j.consistency.checking;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.kernel.impl.store.PreAllocatedRecords;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.mockito.Mockito.verify;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;

public class NodeDynamicLabelOrphanChainStartCheckTest
        extends RecordCheckTestBase<DynamicRecord, DynamicLabelConsistencyReport, NodeDynamicLabelOrphanChainStartCheck>
{

    public static final PreAllocatedRecords RECORD_ALLOCATOR = new PreAllocatedRecords( 66 );

    public NodeDynamicLabelOrphanChainStartCheckTest()
    {
        super( new NodeDynamicLabelOrphanChainStartCheck(), DynamicLabelConsistencyReport.class, new int[0] );
    }

    @Test @Ignore("2013-07-17 Revisit once we store sorted label ids")
    public void shouldReportOrphanRecordsThatAreNotFirst() throws Exception
    {
        // given
        DynamicRecord record0 = addNodeDynamicLabels( inUse( new DynamicRecord( 0 ) ) );
        DynamicRecord record1 = addNodeDynamicLabels( inUse( new DynamicRecord( 1 ) ) );
        DynamicRecord record2 = addNodeDynamicLabels( inUse( new DynamicRecord( 2 ) ) );
        long[] longs = new long[130];
        for ( int i = 0; i < longs.length; i++ )
        {
            longs[i] = i;
        }
        allocateFromNumbers( new ArrayList<DynamicRecord>(), longs, iterator( record0, record1, record2 ),
                RECORD_ALLOCATOR );
        record0.setInUse( false );

        // when
        DynamicLabelConsistencyReport report = check( record1 );

        // then
        verify( report ).orphanDynamicLabelRecord();
    }

    @Test
    public void shouldReportMissingOwnerId() throws Exception
    {
        // given
        DynamicRecord nodeDynamicLabelRecord = inUse( new DynamicRecord( 0 ) ) ;
        allocateFromNumbers( new ArrayList<DynamicRecord>(), new long[] { }, iterator( nodeDynamicLabelRecord ),
                RECORD_ALLOCATOR );

        // when
        DynamicLabelConsistencyReport report = check( nodeDynamicLabelRecord );

        // then
        verify( report ).orphanDynamicLabelRecord();
    }

    @Test
    public void shouldReportOwningNodeRecordNotInUse() throws Exception
    {
        // given
        NodeRecord nodeRecord = notInUse( new NodeRecord( 12l, false, -1, -1 ) );
        add( nodeRecord );

        DynamicRecord nodeDynamicLabelRecord = inUse( new DynamicRecord( 0 ) );
        allocateFromNumbers( new ArrayList<DynamicRecord>(), new long[]{12l}, iterator( nodeDynamicLabelRecord ),
                RECORD_ALLOCATOR );

        // when
        DynamicLabelConsistencyReport report = check( nodeDynamicLabelRecord );

        // then
        verify( report ).orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
    }

    @Test
    public void shouldReportOwningNodeRecordNotPointingBack() throws Exception
    {
        // given
        long nodeId = 12l;

        Collection<DynamicRecord> validLabelRecords = new ArrayList<>();
        allocateFromNumbers( validLabelRecords, new long[] {nodeId}, iterator( inUse( new DynamicRecord( 0 ) ) ),
                RECORD_ALLOCATOR );

        Collection<DynamicRecord> fakePointedToRecords = new ArrayList<>();
        allocateFromNumbers( fakePointedToRecords, new long[] {nodeId}, iterator( inUse( new DynamicRecord( 1 ) ) ), RECORD_ALLOCATOR );

        NodeRecord nodeRecord = inUse( new NodeRecord( nodeId, false, -1, -1 ) );
        nodeRecord.setLabelField( dynamicPointer( fakePointedToRecords ), fakePointedToRecords );
        add( nodeRecord );

        // when
        DynamicLabelConsistencyReport report = check( single( validLabelRecords.iterator() ) );

        // then
        verify( report ).orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
    }
}
