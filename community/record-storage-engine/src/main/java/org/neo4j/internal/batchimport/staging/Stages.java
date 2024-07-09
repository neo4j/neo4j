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

import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.batchimport.RecordIdIterator;
import org.neo4j.internal.batchimport.UpdateRecordsStep;
import org.neo4j.internal.batchimport.store.PrepareIdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class Stages {
    public static <RECORD extends AbstractBaseRecord> Stage classicParallelStoreProcessingStage(
            String name,
            Configuration configuration,
            int orderingGuarantees,
            RecordStore<RECORD> store,
            ThrowingConsumer<RECORD, Throwable> processor,
            RecordIdIterator ids,
            boolean inWriteStage,
            CursorContextFactory contextFactory) {
        return new Stage(name, null, configuration, orderingGuarantees) {
            {
                add(new BatchFeedStep(control(), configuration, ids, store.getRecordSize()));
                add(new ReadRecordsStep<>(control(), configuration, inWriteStage, store, contextFactory));
                add(new ProcessorStep<RECORD[]>(control(), name, configuration, 0, contextFactory) {
                    @Override
                    protected void process(RECORD[] batch, BatchSender sender, CursorContext cursorContext)
                            throws Throwable {
                        if (batch != null) {
                            for (RECORD record : batch) {
                                processor.accept(record);
                            }
                        }
                    }
                });
            }
        };
    }

    public static <RECORD extends AbstractBaseRecord> Stage classicParallelStoreUpdateStage(
            String name,
            Configuration configuration,
            int orderingGuarantees,
            RecordStore<RECORD> store,
            CursorType cursorType,
            Function<CursorContext, StoreCursors> storeCursorsFunction,
            PrepareIdSequence idSequence,
            ThrowingConsumer<RECORD[], Throwable> processor,
            RecordIdIterator ids,
            boolean inWriteStage,
            CursorContextFactory contextFactory) {
        return new Stage(name, null, configuration, orderingGuarantees) {
            {
                add(new BatchFeedStep(control(), configuration, ids, store.getRecordSize()));
                add(new ReadRecordsStep<>(control(), configuration, inWriteStage, store, contextFactory));
                add(new ProcessorStep<RECORD[]>(control(), name, configuration, 0, contextFactory) {
                    @Override
                    protected void process(RECORD[] batch, BatchSender sender, CursorContext cursorContext)
                            throws Throwable {
                        if (batch != null) {
                            processor.accept(batch);
                            sender.send(batch);
                        }
                    }
                });
                add(new UpdateRecordsStep<>(
                        control(), configuration, store, idSequence, contextFactory, storeCursorsFunction, cursorType));
            }
        };
    }
}
