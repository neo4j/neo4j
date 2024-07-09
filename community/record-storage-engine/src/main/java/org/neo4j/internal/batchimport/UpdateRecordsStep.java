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

import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;

import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.stats.Key;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.Stat;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.batchimport.store.PrepareIdSequence;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Updates a batch of records to a store.
 */
public class UpdateRecordsStep<RECORD extends AbstractBaseRecord> extends ProcessorStep<RECORD[]>
        implements StatsProvider {
    protected final RecordStore<RECORD> store;
    private final int recordSize;
    private final Function<CursorContext, StoreCursors> storeCursorsCreator;
    private final CursorType cursorType;
    private final PrepareIdSequence prepareIdSequence;
    private final LongAdder recordsUpdated = new LongAdder();

    public UpdateRecordsStep(
            StageControl control,
            Configuration config,
            RecordStore<RECORD> store,
            PrepareIdSequence prepareIdSequence,
            CursorContextFactory contextFactory,
            Function<CursorContext, StoreCursors> storeCursorsCreator,
            CursorType cursorType) {
        super(control, "v", config, config.parallelRecordWrites() ? 0 : 1, contextFactory);
        this.store = store;
        this.prepareIdSequence = prepareIdSequence;
        this.recordSize = store.getRecordSize();
        this.storeCursorsCreator = storeCursorsCreator;
        this.cursorType = cursorType;
    }

    @Override
    protected void process(RECORD[] batch, BatchSender sender, CursorContext cursorContext) {
        LongFunction<IdSequence> idSequence = prepareIdSequence.apply(store.getIdGenerator());
        int recordsUpdatedInThisBatch = 0;
        try (var storeCursors = storeCursorsCreator.apply(cursorContext);
                var cursor = storeCursors.writeCursor(cursorType)) {
            for (RECORD record : batch) {
                if (record != null && record.inUse() && !IdValidator.isReservedId(record.getId())) {
                    store.prepareForCommit(record, idSequence.apply(record.getId()), cursorContext);
                    // Don't update id generators because at the time of writing this they require special handling for
                    // multi-threaded updates
                    // instead just note the highId. It will be mostly correct in the end.
                    store.updateRecord(record, IGNORE, cursor, cursorContext, storeCursors);
                    recordsUpdatedInThisBatch++;
                }
            }
        }
        recordsUpdated.add(recordsUpdatedInThisBatch);
    }

    @Override
    protected void collectStatsProviders(Collection<StatsProvider> into) {
        super.collectStatsProviders(into);
        into.add(this);
    }

    @Override
    public Stat stat(Key key) {
        return key == Keys.io_throughput
                ? new IoThroughputStat(startTime, endTime, recordSize * recordsUpdated.sum())
                : null;
    }

    @Override
    public Key[] keys() {
        return new Keys[] {Keys.io_throughput};
    }
}
