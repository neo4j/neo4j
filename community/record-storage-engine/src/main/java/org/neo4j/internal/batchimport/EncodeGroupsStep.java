/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * Takes cached {@link RelationshipGroupRecord relationship groups} and sets real ids and
 * {@link RelationshipGroupRecord#getNext() next pointers}, making them ready for writing to store.
 */
public class EncodeGroupsStep extends ProcessorStep<RelationshipGroupRecord[]> {
    private final IdGenerator idGenerator;
    private long nextId = -1;
    private final RecordStore<RelationshipGroupRecord> store;

    public EncodeGroupsStep(
            StageControl control,
            Configuration config,
            RecordStore<RelationshipGroupRecord> store,
            CursorContextFactory contextFactory) {
        super(control, "ENCODE", config, 1, contextFactory);
        this.store = store;
        this.idGenerator = store.getIdGenerator();
    }

    @Override
    protected void process(RelationshipGroupRecord[] batch, BatchSender sender, CursorContext cursorContext) {
        int groupStartIndex = 0;
        for (int i = 0; i < batch.length; i++) {
            RelationshipGroupRecord group = batch[i];

            // The iterator over the groups will not produce real next pointers, they are instead
            // a count meaning how many groups come after it. This encoder will set the real group ids.
            long count = group.getNext();
            boolean lastInChain = count == 0;

            group.setId(nextId == -1 ? nextId = idGenerator.nextId(cursorContext) : nextId);
            if (!lastInChain) {
                group.setNext(nextId = idGenerator.nextId(cursorContext));
            } else {
                group.setNext(nextId = -1);

                // OK so this group is the last in this chain, which means all the groups in this chain
                // are now fully populated. We can now prepare these groups so that their potential
                // secondary units ends up very close by.
                for (int j = groupStartIndex; j <= i; j++) {
                    store.prepareForCommit(batch[j], idGenerator, cursorContext);
                }

                groupStartIndex = i + 1;
            }
        }
        assert groupStartIndex == batch.length;

        sender.send(batch);
    }
}
