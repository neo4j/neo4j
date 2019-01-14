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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.ReadGroupsFromCacheStepTest.Group;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

public class EncodeGroupsStepTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldEncodeGroupChains() throws Throwable
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        final AtomicLong nextId = new AtomicLong();
        RecordStore<RelationshipGroupRecord> store = mock( RecordStore.class );
        when( store.nextId() ).thenAnswer( invocation -> nextId.incrementAndGet() );
        doAnswer( invocation ->
        {
            // our own way of marking that this has record been prepared (firstOut=1)
            invocation.<RelationshipGroupRecord>getArgument( 0 ).setFirstOut( 1 );
            return null;
        } ).when( store ).prepareForCommit( any( RelationshipGroupRecord.class ) );
        Configuration config = Configuration.withBatchSize( DEFAULT, 10 );
        EncodeGroupsStep encoder = new EncodeGroupsStep( control, config, store );

        // WHEN
        encoder.start( Step.ORDER_SEND_DOWNSTREAM );
        Catcher catcher = new Catcher();
        encoder.process( batch( new Group( 1, 3 ), new Group( 2, 3 ), new Group( 3, 4 ) ), catcher );
        encoder.process( batch( new Group( 4, 2 ), new Group( 5, 10 ) ), catcher );
        encoder.process( batch( new Group( 6, 35 ) ), catcher );
        encoder.process( batch( new Group( 7, 2 ) ), catcher );
        encoder.endOfUpstream();
        while ( !encoder.isCompleted() )
        {
            Thread.sleep( 10 );
        }
        encoder.close();

        // THEN
        assertEquals( 4, catcher.batches.size() );
        long lastOwningNodeLastBatch = -1;
        for ( RelationshipGroupRecord[] batch : catcher.batches )
        {
            assertBatch( batch, lastOwningNodeLastBatch );
            lastOwningNodeLastBatch = batch[batch.length - 1].getOwningNode();
        }
    }

    private void assertBatch( RelationshipGroupRecord[] batch, long lastOwningNodeLastBatch )
    {
        for ( int i = 0; i < batch.length; i++ )
        {
            RelationshipGroupRecord record = batch[i];
            assertTrue( record.getId() > Record.NULL_REFERENCE.longValue() );
            assertTrue( record.getOwningNode() > lastOwningNodeLastBatch );
            assertEquals( 1, record.getFirstOut() ); // the mark our store mock sets when preparing
            if ( record.getNext() == Record.NULL_REFERENCE.longValue() )
            {
                // This is the last in the chain, verify that this is either:
                assertTrue(
                        // - the last one in the batch, or
                        i == batch.length - 1 ||
                        // - the last one for this node
                        batch[i + 1].getOwningNode() > record.getOwningNode() );
            }
        }
    }

    private RelationshipGroupRecord[] batch( Group... groups )
    {
        return ReadGroupsFromCacheStepTest.groups( groups ).toArray( new RelationshipGroupRecord[0] );
    }

    private static class Catcher implements BatchSender
    {
        private final List<RelationshipGroupRecord[]> batches = new ArrayList<>();

        @Override
        public void send( Object batch )
        {
            batches.add( (RelationshipGroupRecord[]) batch );
        }
    }
}
