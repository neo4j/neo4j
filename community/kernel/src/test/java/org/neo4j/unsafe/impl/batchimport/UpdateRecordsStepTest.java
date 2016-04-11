/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateRecordsStepTest
{
    @Test
    public void ioThroughputStatDoesNotOverflow() throws Throwable
    {
        // store with huge record size to force overflow and not create huge batch of records
        RecordStore<NodeRecord> store = mock( RecordStore.class );
        when( store.getRecordSize() ).thenReturn( Integer.MAX_VALUE / 2 );

        Configuration configuration = mock( Configuration.class );
        StageControl stageControl = mock( StageControl.class );
        UpdateRecordsStep<NodeRecord> step = new UpdateRecordsStep<>( stageControl, configuration, store );

        NodeRecord record = new NodeRecord( 1 );
        record.setInUse( true );
        NodeRecord[] batch = new NodeRecord[11];
        Arrays.fill( batch, record );

        step.process( batch, mock( BatchSender.class ) );

        Stat stat = step.stat( Keys.io_throughput );

        assertThat( stat.asLong(), greaterThan( 0L ) );
    }

    @Test
    public void recordWithReservedIdIsSkipped() throws Throwable
    {
        RecordStore<NodeRecord> store = mock( NodeStore.class );
        StageControl stageControl = mock( StageControl.class );
        UpdateRecordsStep<NodeRecord> step = new UpdateRecordsStep<>( stageControl, Configuration.DEFAULT, store );

        NodeRecord node1 = new NodeRecord( 1 );
        NodeRecord node2 = new NodeRecord( 2 );
        NodeRecord nodeWithReservedId = new NodeRecord( IdGeneratorImpl.INTEGER_MINUS_ONE );
        NodeRecord[] batch = {node1, node2, nodeWithReservedId};

        step.process( batch, mock( BatchSender.class ) );

        verify( store ).prepareForCommit( node1 );
        verify( store ).updateRecord( node1 );
        verify( store ).prepareForCommit( node2 );
        verify( store ).updateRecord( node2 );
        verify( store, never() ).prepareForCommit( nodeWithReservedId );
        verify( store, never() ).updateRecord( nodeWithReservedId );
    }
}
