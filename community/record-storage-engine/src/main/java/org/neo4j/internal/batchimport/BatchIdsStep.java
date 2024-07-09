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

import java.util.Arrays;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.ProducerStep;
import org.neo4j.internal.batchimport.staging.StageControl;

class BatchIdsStep extends ProducerStep {
    private final LongIterator ids;
    private long position;

    BatchIdsStep(StageControl control, Configuration config, LongIterator ids) {
        super(control, config);
        this.ids = ids;
    }

    @Override
    protected void process() {
        var batch = new long[config.batchSize()];
        var batchIndex = 0;
        while (ids.hasNext()) {
            if (batchIndex == batch.length) {
                send(batch);
                batch = new long[config.batchSize()];
                batchIndex = 0;
            }

            batch[batchIndex++] = ids.next();
        }
        if (batchIndex > 0) {
            send(Arrays.copyOf(batch, batchIndex));
        }
    }

    private void send(long[] batch) {
        sendDownstream(batch);
        position += batch.length;
    }

    @Override
    protected long position() {
        return position;
    }
}
