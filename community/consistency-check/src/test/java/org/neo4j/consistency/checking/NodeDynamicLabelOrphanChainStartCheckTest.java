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
package org.neo4j.consistency.checking;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;

public class NodeDynamicLabelOrphanChainStartCheckTest
        extends RecordCheckTestBase<DynamicRecord, DynamicLabelConsistencyReport, NodeDynamicLabelOrphanChainStartCheck>
{

    public NodeDynamicLabelOrphanChainStartCheckTest()
    {
        super( new NodeDynamicLabelOrphanChainStartCheck(), DynamicLabelConsistencyReport.class, new int[0] );
    }

    @Test
    public void shouldReportMissingOwnerId()
    {
        // given
        DynamicRecord record = new DynamicRecord( 0 );
        inUse( record ) ;
        allocateFromNumbers( new ArrayList<>(), new long[] { }, new ReusableRecordsAllocator( 66, record ) );

        // when
        DynamicLabelConsistencyReport report = check( record );

        // then
        verify( report ).orphanDynamicLabelRecord();
    }

    @Test
    public void shouldReportOwningNodeRecordNotInUse()
    {
        // given
        NodeRecord nodeRecord = notInUse( new NodeRecord( 12L, false, -1, -1 ) );
        add( nodeRecord );

        DynamicRecord nodeDynamicLabelRecord = inUse( new DynamicRecord( 0 ) );
        allocateFromNumbers( new ArrayList<>(), new long[]{12L}, new ReusableRecordsAllocator( 66, nodeDynamicLabelRecord ) );

        // when
        DynamicLabelConsistencyReport report = check( nodeDynamicLabelRecord );

        // then
        verify( report ).orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
    }

    @Test
    public void shouldReportOwningNodeRecordNotPointingBack()
    {
        // given
        long nodeId = 12L;

        Collection<DynamicRecord> validLabelRecords = new ArrayList<>();
        DynamicRecord dynamicRecord = inUse( new DynamicRecord( 0 ) );
        allocateFromNumbers( validLabelRecords, new long[] {nodeId}, new ReusableRecordsAllocator( 66, dynamicRecord ) );

        Collection<DynamicRecord> fakePointedToRecords = new ArrayList<>();
        DynamicRecord dynamicRecord1 = inUse( new DynamicRecord( 1 ) );
        allocateFromNumbers( fakePointedToRecords, new long[] {nodeId}, new ReusableRecordsAllocator( 66, dynamicRecord1 ) );

        NodeRecord nodeRecord = inUse( new NodeRecord( nodeId, false, -1, -1 ) );
        nodeRecord.setLabelField( dynamicPointer( fakePointedToRecords ), fakePointedToRecords );
        add( nodeRecord );

        // when
        DynamicLabelConsistencyReport report = check( Iterators.single( validLabelRecords.iterator() ) );

        // then
        verify( report ).orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
    }
}
