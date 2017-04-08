/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.unsafe.impl.batchimport.StoreWithReservedId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.Configuration.withBatchSize;
import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.allIn;

public class ReadRecordsStepTest
{
    @Test
    public void reservedIdIsSkipped()
    {
        long highId = 5;
        int batchSize = (int) highId;
        org.neo4j.unsafe.impl.batchimport.Configuration config = withBatchSize( DEFAULT, batchSize );
        NodeStore store = StoreWithReservedId.newNodeStoreMock( highId );
        when( store.getHighId() ).thenReturn( highId );
        when( store.getRecordsPerPage() ).thenReturn( 10 );

        ReadRecordsStep<NodeRecord> step = new ReadRecordsStep<>( mock( StageControl.class ), config,
                store, allIn( store, config ) );
        step.start( 0 );

        Object batch = step.nextBatchOrNull( 0, batchSize );

        assertNotNull( batch );

        NodeRecord[] records = (NodeRecord[]) batch;
        boolean hasRecordWithReservedId = Stream.of( records ).anyMatch( recordWithReservedId() );
        assertFalse( "Batch contains record with reserved id " + Arrays.toString( records ), hasRecordWithReservedId );
    }

    @Test
    public void shouldContinueThroughBigIdHoles() throws Exception
    {
        // GIVEN
        NodeStore store = mock( NodeStore.class );
        long highId = 100L;
        when( store.getHighId() ).thenReturn( highId );
        when( store.newRecord() ).thenReturn( new NodeRecord( -1 ) );
        org.neo4j.unsafe.impl.batchimport.Configuration config = withBatchSize( DEFAULT, 10 );
        when( store.newRecordCursor( any( NodeRecord.class ) ) ).thenAnswer( invocation ->
        {
            return new ControlledRecordCursor<>( (NodeRecord) invocation.getArguments()[0], record ->
            {
                record.setInUse(
                        record.getId() < config.batchSize() || record.getId() >= highId - config.batchSize() / 2 );
                return record.inUse() && record.getId() < highId;
            } );
        } );
        ReadRecordsStep<NodeRecord> step = new ReadRecordsStep<>( mock( StageControl.class ),
                config, store, allIn( store, config ) );
        step.start( 0 );

        // WHEN
        NodeRecord[] first = (NodeRecord[]) step.nextBatchOrNull( 0, config.batchSize() );
        NodeRecord[] second = (NodeRecord[]) step.nextBatchOrNull( 1, config.batchSize() );
        NodeRecord[] third = (NodeRecord[]) step.nextBatchOrNull( 2, config.batchSize() );

        // THEN
        assertEquals( config.batchSize(), first.length );
        assertEquals( 0L, first[0].getId() );
        assertEquals( first[0].getId() + config.batchSize() - 1, first[first.length - 1].getId() );

        assertEquals( config.batchSize() / 2, second.length );
        assertEquals( highId - 1, second[second.length - 1].getId() );

        assertNull( third );
    }

    private static Predicate<NodeRecord> recordWithReservedId()
    {
        return record -> record.getId() == IdGeneratorImpl.INTEGER_MINUS_ONE;
    }

    private static class ControlledRecordCursor<RECORD extends AbstractBaseRecord> implements RecordCursor<RECORD>
    {
        private final RECORD record;
        private final Predicate<RECORD> populator;

        ControlledRecordCursor( RECORD record, Predicate<RECORD> populator )
        {
            this.record = record;
            this.populator = populator;
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public RECORD get()
        {
            return record;
        }

        @Override
        public RecordCursor<RECORD> acquire( long id, RecordLoad mode )
        {
            return this;
        }

        @Override
        public void placeAt( long id, RecordLoad mode )
        {   // Don't care about this, we only care about next(id) in this class
        }

        @Override
        public boolean next()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean next( long id )
        {
            record.setId( id );
            return populator.test( record );
        }

        @Override
        public boolean next( long id, RECORD record, RecordLoad mode )
        {
            throw new UnsupportedOperationException();
        }
    }
}
