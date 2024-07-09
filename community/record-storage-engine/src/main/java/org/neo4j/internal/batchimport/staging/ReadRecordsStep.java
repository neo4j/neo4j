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
package org.neo4j.internal.batchimport.staging;

import static java.lang.Integer.min;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Reads records from a {@link RecordStore} and sends batches of those records downstream.
 * A {@link PageCursor} is used during the life cycle of this {@link Step}, e.g. between
 * {@link #start(int)} and {@link #close()}.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
public class ReadRecordsStep<RECORD extends AbstractBaseRecord> extends ProcessorStep<LongIterator> {
    public static final Monitor NO_MONITOR = id -> {};

    private final RecordStore<RECORD> store;
    private final int batchSize;
    private final Monitor monitor;
    private final RecordDataAssembler<RECORD> assembler;

    public ReadRecordsStep(
            StageControl control,
            Configuration config,
            boolean inRecordWritingStage,
            RecordStore<RECORD> store,
            CursorContextFactory contextFactory) {
        this(control, config, inRecordWritingStage, store, contextFactory, defaultAssembler(store), NO_MONITOR);
    }

    public ReadRecordsStep(
            StageControl control,
            Configuration config,
            boolean inRecordWritingStage,
            RecordStore<RECORD> store,
            CursorContextFactory contextFactory,
            RecordDataAssembler<RECORD> converter,
            Monitor monitor) {
        super(
                control,
                ">",
                config,
                parallelReading(config, inRecordWritingStage)
                        // Limit reader (I/O) threads to 12, it's a high degree of concurrency and assigning more
                        // will likely not make things faster, rather the other way around and it's difficult for
                        // the processor assigner to proficiently understand that dynamic
                        ? min(12, config.maxNumberOfWorkerThreads())
                        : 1,
                contextFactory);
        this.store = store;
        this.assembler = converter;
        this.batchSize = config.batchSize();
        this.monitor = monitor;
    }

    private static boolean parallelReading(Configuration config, boolean inRecordWritingStage) {
        return (inRecordWritingStage && config.highIO()) || (!inRecordWritingStage && config.parallelRecordReads());
    }

    @Override
    public void start(int orderingGuarantees) {
        super.start(orderingGuarantees | ORDER_SEND_DOWNSTREAM);
    }

    @Override
    protected void process(LongIterator idRange, BatchSender sender, CursorContext cursorContext) {
        if (!idRange.hasNext()) {
            return;
        }

        long id = idRange.next();
        RECORD[] batch = control.reuse(() -> assembler.newBatchObject(batchSize));
        int i = 0;
        // Just use the first record in the batch here to satisfy the record cursor.
        // The truth is that we'll be using the read method which accepts an external record anyway so it doesn't
        // matter.
        try (PageCursor cursor = store.openPageCursorForReading(id, cursorContext)) {
            boolean hasNext = true;
            while (hasNext) {
                if (assembler.append(store, cursor, batch, id, i)) {
                    i++;
                } else {
                    monitor.recordSkipped(id);
                }
                if (hasNext = idRange.hasNext()) {
                    id = idRange.next();
                }
            }
        }

        sender.send(assembler.cutOffAt(batch, i));
    }

    public interface Monitor {
        void recordSkipped(long id);
    }

    public static <RECORD extends AbstractBaseRecord> RecordDataAssembler<RECORD> defaultAssembler(
            RecordStore<RECORD> store) {
        return new RecordDataAssembler<>(store::newRecord, true);
    }
}
