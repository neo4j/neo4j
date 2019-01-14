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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Takes cached {@link RelationshipGroupRecord relationship groups} and sets real ids and
 * {@link RelationshipGroupRecord#getNext() next pointers}, making them ready for writing to store.
 */
public class EncodeGroupsStep extends ProcessorStep<RelationshipGroupRecord[]>
{
    private long nextId = -1;
    private final RecordStore<RelationshipGroupRecord> store;

    public EncodeGroupsStep( StageControl control, Configuration config, RecordStore<RelationshipGroupRecord> store )
    {
        super( control, "ENCODE", config, 1 );
        this.store = store;
    }

    @Override
    protected void process( RelationshipGroupRecord[] batch, BatchSender sender )
    {
        int groupStartIndex = 0;
        for ( int i = 0; i < batch.length; i++ )
        {
            RelationshipGroupRecord group = batch[i];

            // The iterator over the groups will not produce real next pointers, they are instead
            // a count meaning how many groups come after it. This encoder will set the real group ids.
            long count = group.getNext();
            boolean lastInChain = count == 0;

            group.setId( nextId == -1 ? nextId = store.nextId() : nextId );
            if ( !lastInChain )
            {
                group.setNext( nextId = store.nextId() );
            }
            else
            {
                group.setNext( nextId = -1 );

                // OK so this group is the last in this chain, which means all the groups in this chain
                // are now fully populated. We can now prepare these groups so that their potential
                // secondary units ends up very close by.
                for ( int j = groupStartIndex; j <= i; j++ )
                {
                    store.prepareForCommit( batch[j] );
                }

                groupStartIndex = i + 1;
            }
        }
        assert groupStartIndex == batch.length;

        sender.send( batch );
    }
}
