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

import static org.neo4j.internal.helpers.collection.Iterators.prefetching;

import java.util.Arrays;
import java.util.Iterator;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.ProcessContext;
import org.neo4j.internal.batchimport.staging.PullingProducerStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * Reads {@link RelationshipGroupRecord group records} from {@link RelationshipGroupCache}, sending
 * them downstream in batches.
 */
public class ReadGroupsFromCacheStep extends PullingProducerStep<ProcessContext> {
    private final int itemSize;
    private final PrefetchingIterator<RelationshipGroupRecord> data;
    private RelationshipGroupRecord[] scratch;
    private int cursor;

    public ReadGroupsFromCacheStep(
            StageControl control, Configuration config, Iterator<RelationshipGroupRecord> groups, int itemSize) {
        super(control, config);
        this.data = prefetching(groups);
        this.itemSize = itemSize;
        this.scratch = new RelationshipGroupRecord[config.batchSize() * 2]; // grows on demand
    }

    @Override
    protected Object nextBatchOrNull(long ticket, int batchSize, ProcessContext processContext) {
        if (!data.hasNext()) {
            return null;
        }

        int i = 0;
        long lastOwner = -1;
        for (; data.hasNext(); i++) {
            // Logic below makes it so that all groups for a specific node ends up in the same batch,
            // which means that batches are slightly bigger (varying) than the requested size.
            RelationshipGroupRecord item = data.peek();
            if (i == batchSize - 1) {
                // Remember which owner this "last" group has...
                lastOwner = item.getOwningNode();
            } else if (i >= batchSize) {
                // ...and continue including groups in this batch until next owner comes
                if (item.getOwningNode() != lastOwner) {
                    break;
                }
            }

            if (i >= scratch.length) {
                scratch = Arrays.copyOf(scratch, scratch.length * 2);
            }
            scratch[i] = data.next(); // which is "item", but also advances the iterator
            cursor++;
        }
        return Arrays.copyOf(scratch, i);
    }

    @Override
    protected long position() {
        return cursor * itemSize;
    }
}
