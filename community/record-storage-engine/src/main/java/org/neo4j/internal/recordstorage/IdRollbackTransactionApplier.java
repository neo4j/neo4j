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
package org.neo4j.internal.recordstorage;

import static org.neo4j.collection.diffset.TrackableDiffSets.newMutableLongDiffSets;
import static org.neo4j.internal.recordstorage.RecordIdType.ARRAY_BLOCK;
import static org.neo4j.internal.recordstorage.RecordIdType.NODE_LABELS;
import static org.neo4j.internal.recordstorage.RecordIdType.STRING_BLOCK;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.neo4j.collection.diffset.MutableLongDiffSets;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.memory.EmptyMemoryTracker;

public class IdRollbackTransactionApplier extends TransactionApplier.Adapter {

    private final IdGeneratorFactory idGeneratorFactory;
    private final CursorContext cursorContext;
    private final Map<RecordIdType, MutableLongDiffSets> idMaps = new EnumMap<>(RecordIdType.class);

    public IdRollbackTransactionApplier(IdGeneratorFactory idGeneratorFactory, CursorContext cursorContext) {
        this.idGeneratorFactory = idGeneratorFactory;
        this.cursorContext = cursorContext;
    }

    @Override
    public boolean visitNodeCommand(Command.NodeCommand command) {
        checkId(command, RecordIdType.NODE);
        checkDynamicLabels(command);
        return false;
    }

    private void checkDynamicLabels(Command.NodeCommand command) {
        var dynamicRecordsBefore = command.getBefore().getDynamicLabelRecords();
        var dynamicRecordsAfter = command.getAfter().getDynamicLabelRecords();
        if (!dynamicRecordsAfter.isEmpty() || !dynamicRecordsBefore.isEmpty()) {
            var diffSet = idMaps.computeIfAbsent(NODE_LABELS, type -> getLongDiffSets());
            markIds(dynamicRecordsAfter, diffSet);
            markIdsUsed(dynamicRecordsBefore, diffSet);
        }
    }

    @Override
    public boolean visitRelationshipCommand(Command.RelationshipCommand command) {
        checkId(command, RecordIdType.RELATIONSHIP);
        return false;
    }

    @Override
    public boolean visitPropertyCommand(Command.PropertyCommand command) {
        checkId(command, RecordIdType.PROPERTY);
        var stringBlockDiffs = idMaps.computeIfAbsent(STRING_BLOCK, type -> getLongDiffSets());
        var arrayBlockDiffs = idMaps.computeIfAbsent(ARRAY_BLOCK, type -> getLongDiffSets());

        for (PropertyBlock block : command.getAfter().propertyBlocks()) {
            switch (block.getType()) {
                case STRING -> markIds(block.getValueRecords(), stringBlockDiffs);
                case ARRAY -> markIds(block.getValueRecords(), arrayBlockDiffs);
                default -> {
                    // Not needed, no dynamic records then
                }
            }
        }

        for (PropertyBlock block : command.getBefore().propertyBlocks()) {
            switch (block.getType()) {
                case STRING -> markIdsUsed(block.getValueRecords(), stringBlockDiffs);
                case ARRAY -> markIdsUsed(block.getValueRecords(), arrayBlockDiffs);
                default -> {
                    // Not needed, no dynamic records then
                }
            }
        }
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand(Command.RelationshipGroupCommand command) {
        checkId(command, RecordIdType.RELATIONSHIP_GROUP);
        return false;
    }

    private <T extends AbstractBaseRecord> void checkId(Command.BaseCommand<T> command, RecordIdType idType) {
        T commandAfter = command.getAfter();
        if (commandAfter.isCreated()) {
            idMaps.computeIfAbsent(idType, type -> getLongDiffSets()).remove(commandAfter.getId());
        } else if (!commandAfter.inUse()) {
            idMaps.computeIfAbsent(idType, type -> getLongDiffSets()).add(commandAfter.getId());
        }
    }

    @Override
    public void close() throws Exception {
        idGeneratorFactory.notifyTransactionRollback(
                cursorContext.getVersionContext().committingTransactionId());
        idMaps.forEach((type, longDiffSets) -> {
            IdGenerator idGenerator = idGeneratorFactory.get(type);
            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                longDiffSets.getAdded().forEach(marker::markUsed);
                longDiffSets.getRemoved().forEach(marker::markDeletedAndFree);
            }
        });
    }

    private static MutableLongDiffSets getLongDiffSets() {
        return newMutableLongDiffSets(OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE);
    }

    private static void markIdsUsed(List<DynamicRecord> block, MutableLongDiffSets diffs) {
        for (DynamicRecord valueRecord : block) {
            diffs.add(valueRecord.getId());
        }
    }

    private static void markIds(List<DynamicRecord> dynamicRecordsAfter, MutableLongDiffSets diffSet) {
        for (DynamicRecord dynamicRecord : dynamicRecordsAfter) {
            if (dynamicRecord.inUse()) {
                diffSet.remove(dynamicRecord.getId());
            } else {
                diffSet.add(dynamicRecord.getId());
            }
        }
    }
}
