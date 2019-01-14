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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

public class ReadGroupsFromCacheStepTest
{
    @Test
    public void shouldProduceCompleteBatchesPerOwner()
    {
        // GIVEN
        Configuration config = Configuration.withBatchSize( DEFAULT, 10 );
        Iterator<RelationshipGroupRecord> groups = groups(
                new Group( 1, 3 ),
                new Group( 2, 3 ),
                new Group( 3, 4 ),  // ^^^ perfect batch size
                new Group( 4, 2 ),
                new Group( 5, 10 ), // ^^^ slightly bigger than batch size
                new Group( 6, 35 ), // ^^^ much bigger than batch size
                new Group( 7, 2 ) ).iterator();
        final AtomicInteger processCounter = new AtomicInteger();
        Stage stage = new Stage( getClass().getSimpleName(), null, config, 0 )
        {
            {
                add( new ReadGroupsFromCacheStep( control(), config, groups, 1 ) );
                add( new VerifierStep( control(), config, processCounter ) );
            }
        };

        // WHEN processing the data
        superviseDynamicExecution( stage );

        // THEN
        assertEquals( 4, processCounter.get() );
    }

    protected static List<RelationshipGroupRecord> groups( Group... groups )
    {
        List<RelationshipGroupRecord> records = new ArrayList<>();
        for ( Group group : groups )
        {
            for ( int i = 0; i < group.count; i++ )
            {
                RelationshipGroupRecord record = new RelationshipGroupRecord( NULL_REFERENCE.longValue() );
                record.setOwningNode( group.owningNode );
                record.setNext( group.count - i - 1 ); // count: how many come after it (importer does this)
                records.add( record );
            }
        }
        return records;
    }

    protected static class Group
    {
        final long owningNode;
        final int count;

        public Group( long owningNode, int count )
        {
            this.owningNode = owningNode;
            this.count = count;
        }
    }

    private static class VerifierStep extends ProcessorStep<RelationshipGroupRecord[]>
    {
        private long lastBatchLastOwningNode = -1;
        private final AtomicInteger processCounter;

        VerifierStep( StageControl control, Configuration config, AtomicInteger processCounter )
        {
            super( control, "Verifier", config, 1 );
            this.processCounter = processCounter;
        }

        @Override
        protected void process( RelationshipGroupRecord[] batch, BatchSender sender )
        {
            long lastOwningNode = lastBatchLastOwningNode;
            for ( RelationshipGroupRecord record : batch )
            {
                assertTrue( record.getOwningNode() >= lastOwningNode );
                assertTrue( record.getOwningNode() > lastBatchLastOwningNode );
            }
            processCounter.incrementAndGet();
            if ( batch.length > 0 )
            {
                lastBatchLastOwningNode = batch[batch.length - 1].getOwningNode();
            }
        }
    }
}
