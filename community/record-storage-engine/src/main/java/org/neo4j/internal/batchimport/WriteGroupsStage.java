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

import static org.neo4j.internal.batchimport.RelationshipGroupCache.GROUP_ENTRY_SIZE;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;

import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.store.StorePrepareIdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Writes cached {@link RelationshipGroupRecord} from {@link ScanAndCacheGroupsStage} to store. This is done
 * as a separate step because here the cache is supposed to contain complete chains of relationship group records
 * for a section of the node store. Steps:
 *
 * <ol>
 * <li>{@link ReadGroupsFromCacheStep} reads complete relationship group chains from {@link RelationshipGroupCache}.
 * </li>
 * <li>{@link EncodeGroupsStep} sets correct {@link RelationshipGroupRecord#setNext(long)} pointers for records.</li>
 * <li>{@link UpdateRecordsStep} writes the relationship group records to store.</li>
 * </ol>
 */
public class WriteGroupsStage extends Stage {
    public static final String NAME = "Write";

    public WriteGroupsStage(
            Configuration config,
            RelationshipGroupCache cache,
            RecordStore<RelationshipGroupRecord> store,
            CursorContextFactory contextFactory,
            Function<CursorContext, StoreCursors> storeCursorsCreator) {
        super(NAME, null, config, 0);
        add(new ReadGroupsFromCacheStep(control(), config, cache.iterator(), GROUP_ENTRY_SIZE));
        add(new EncodeGroupsStep(control(), config, store, contextFactory));
        add(new UpdateRecordsStep<>(
                control(),
                config,
                store,
                new StorePrepareIdSequence(),
                contextFactory,
                storeCursorsCreator,
                GROUP_CURSOR));
    }
}
