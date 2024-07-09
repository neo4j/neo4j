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

import java.util.ArrayList;
import java.util.List;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.collection.PrimitiveLongCollections.RangedLongIterator;
import org.neo4j.internal.batchimport.cache.ByteArray;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;
import org.neo4j.internal.batchimport.cache.NodeType;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * Using the {@link NodeRelationshipCache} efficiently looks for changed nodes and reads those
 * {@link NodeRecord} and sends downwards.
 */
public class ReadGroupRecordsByCacheStep extends ProcessorStep<RangedLongIterator> {
    private static final String READ_RELATIONSHIP_GROUPS_STEP_TAG = "readRelationshipGroupsStep";
    private final RecordStore<RelationshipGroupRecord> store;
    private final IdGenerator storeIdGenerator;
    private final NodeRelationshipCache cache;

    public ReadGroupRecordsByCacheStep(
            StageControl control,
            Configuration config,
            RecordStore<RelationshipGroupRecord> store,
            NodeRelationshipCache cache,
            CursorContextFactory contextFactory) {
        super(control, ">", config, 0, contextFactory);
        this.store = store;
        this.storeIdGenerator = store.getIdGenerator();
        this.cache = cache;
    }

    @Override
    protected void process(RangedLongIterator batch, BatchSender sender, CursorContext cursorContext) throws Throwable {
        try (var visitor = new NodeVisitor(sender, cursorContext)) {
            cache.visitChangedNodes(visitor, NodeType.NODE_TYPE_DENSE, batch.startInclusive(), batch.endExclusive());
        }
    }

    private class NodeVisitor implements NodeChangeVisitor, AutoCloseable, NodeRelationshipCache.GroupVisitor {
        private final BatchSender sender;
        private final CursorContext cursorContext;
        private List<RelationshipGroupRecord> batch = new ArrayList<>();

        NodeVisitor(BatchSender sender, CursorContext cursorContext) {
            this.sender = sender;
            this.cursorContext = cursorContext;
        }

        @Override
        public void change(long nodeId, ByteArray array) {
            cache.getFirstRel(nodeId, this);
        }

        @Override
        public long visit(long nodeId, int typeId, long out, long in, long loop) {
            long id = storeIdGenerator.nextId(cursorContext);
            RelationshipGroupRecord record = store.newRecord();
            record.setId(id);
            record.initialize(true, typeId, out, in, loop, nodeId, loop);
            batch.add(record);
            if (batch.size() >= config.batchSize()) {
                send();
            }
            return id;
        }

        private void send() {
            sender.send(batch.toArray(new RelationshipGroupRecord[0]));
            batch = new ArrayList<>();
        }

        @Override
        public void close() {
            if (!batch.isEmpty()) {
                send();
            }
        }
    }
}
