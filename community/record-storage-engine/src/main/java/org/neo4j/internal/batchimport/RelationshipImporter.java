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

import static java.lang.String.format;
import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;

import java.util.function.LongFunction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.MissingRelationshipDataException;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.batchimport.store.PrepareIdSequence;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.api.TokenHolder;

/**
 * Imports relationships using data from {@link InputChunk}.
 */
public class RelationshipImporter extends EntityImporter {
    private final TokenHolder relationshipTypeTokenRepository;
    private final IdMapper.Getter idMapper;
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord relationshipRecord;
    private final BatchingIdGetter relationshipIds;
    private final DataStatistics.Client typeCounts;
    private final Collector badCollector;
    private final boolean validateRelationshipData;
    private final boolean doubleRecordUnits;
    private final LongFunction<IdSequence> prepareIdSequence;
    private final PageCursor relationshipUpdateCursor;

    private long relationshipCount;

    // State to keep in the event of bad relationships that need to be handed to the Collector
    private Object startId;
    private Group startIdGroup;
    private Object endId;
    private Group endIdGroup;
    private String type;

    protected RelationshipImporter(
            BatchingNeoStores stores,
            IdMapper idMapper,
            DataStatistics typeDistribution,
            DataImporter.Monitor monitor,
            Collector badCollector,
            boolean validateRelationshipData,
            boolean doubleRecordUnits,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            SchemaMonitor schemaMonitor) {
        super(stores, monitor, contextFactory, memoryTracker, schemaMonitor);
        this.doubleRecordUnits = doubleRecordUnits;
        this.relationshipTypeTokenRepository = stores.getTokenHolders().relationshipTypeTokens();
        this.idMapper = idMapper.newGetter();
        this.badCollector = badCollector;
        this.validateRelationshipData = validateRelationshipData;
        this.relationshipStore = stores.getRelationshipStore();
        this.relationshipRecord = relationshipStore.newRecord();
        this.relationshipIds = batchingIdGetter(relationshipStore);
        this.typeCounts = typeDistribution.newClient();
        this.prepareIdSequence = PrepareIdSequence.of(doubleRecordUnits)
                .apply(stores.getRelationshipStore().getIdGenerator());
        this.relationshipUpdateCursor = relationshipStore.openPageCursorForWriting(0, cursorContext);
        relationshipRecord.setInUse(true);
    }

    @Override
    protected PrimitiveRecord primitiveRecord() {
        return relationshipRecord;
    }

    @Override
    public boolean startId(long id) {
        relationshipRecord.setFirstNode(id);
        return true;
    }

    @Override
    public boolean startId(Object id, Group group) {
        this.startId = id;
        this.startIdGroup = group;

        long nodeId = nodeId(id, group);
        relationshipRecord.setFirstNode(nodeId);
        return true;
    }

    @Override
    public boolean endId(long id) {
        relationshipRecord.setSecondNode(id);
        return true;
    }

    @Override
    public boolean endId(Object id, Group group) {
        this.endId = id;
        this.endIdGroup = group;

        long nodeId = nodeId(id, group);
        relationshipRecord.setSecondNode(nodeId);
        return true;
    }

    private long nodeId(Object id, Group group) {
        long nodeId = idMapper.get(id, group);
        if (nodeId == IdMapper.ID_NOT_FOUND) {
            relationshipRecord.setInUse(false);
            return IdMapper.ID_NOT_FOUND;
        }
        return nodeId;
    }

    @Override
    public boolean type(int typeId) {
        relationshipRecord.setType(typeId);
        schemaMonitor.entityToken(typeId);
        return true;
    }

    @Override
    public boolean type(String type) {
        this.type = type;
        try {
            return type(relationshipTypeTokenRepository.getOrCreateId(type));
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void endOfEntity() {
        if (relationshipRecord.inUse()
                && relationshipRecord.getFirstNode() != IdMapper.ID_NOT_FOUND
                && relationshipRecord.getSecondNode() != IdMapper.ID_NOT_FOUND
                && relationshipRecord.getType() != -1) {
            relationshipRecord.setId(relationshipIds.nextId(cursorContext));
            if (schemaMonitor.endOfEntity(
                    relationshipRecord.getId(),
                    (entityId, tokens, properties, constraintDescription) ->
                            badCollector.collectRelationshipViolatingConstraint(
                                    namedProperties(properties),
                                    constraintDescription,
                                    startId,
                                    startIdGroup,
                                    type,
                                    endId,
                                    endIdGroup))) {
                if (doubleRecordUnits) {
                    // simply reserve one id for this relationship to grow during linking stage
                    relationshipIds.nextId(cursorContext);
                }
                relationshipRecord.setNextProp(createAndWritePropertyChain(cursorContext));
                relationshipRecord.setFirstInFirstChain(false);
                relationshipRecord.setFirstInSecondChain(false);
                relationshipRecord.setFirstPrevRel(Record.NO_NEXT_RELATIONSHIP.intValue());
                relationshipRecord.setSecondPrevRel(Record.NO_NEXT_RELATIONSHIP.intValue());

                relationshipStore.prepareForCommit(
                        relationshipRecord, prepareIdSequence.apply(relationshipRecord.getId()), cursorContext);
                relationshipStore.updateRecord(
                        relationshipRecord, IGNORE, relationshipUpdateCursor, cursorContext, storeCursors);
                relationshipCount++;
                typeCounts.increment(relationshipRecord.getType());
            } else {
                freeUnusedId(relationshipStore, relationshipRecord.getId(), cursorContext);
            }
        } else {
            if (validateRelationshipData) {
                validateNode(startId, Type.START_ID);
                validateNode(endId, Type.END_ID);
                if (relationshipRecord.getType() == -1) {
                    throw new MissingRelationshipDataException(
                            Type.TYPE, relationshipDataString() + " is missing " + Type.TYPE + " field");
                }
            }
            badCollector.collectBadRelationship(
                    idToReport(startId, relationshipRecord.getFirstNode()),
                    startIdGroup,
                    idToReport(type, relationshipRecord.getType()),
                    idToReport(endId, relationshipRecord.getSecondNode()),
                    endIdGroup,
                    relationshipRecord.getFirstNode() == IdMapper.ID_NOT_FOUND ? startId : endId);
            entityPropertyCount = 0;
        }
        reset();
    }

    /**
     * An input ID can be reported either as a "user input id" such as a string or long, temporary to the import,
     * or as an actual internal ID. Reporting bad relationships gets more precise if both are considered.
     */
    private Object idToReport(Object inputId, long recordFieldId) {
        return inputId != null ? inputId : recordFieldId != -1 ? recordFieldId : null;
    }

    @Override
    public void reset() {
        super.reset();
        relationshipRecord.clear();
        relationshipRecord.setInUse(true);
        startId = null;
        startIdGroup = null;
        endId = null;
        endIdGroup = null;
        type = null;
    }

    private void validateNode(Object id, Type fieldType) {
        if (id == null) {
            throw new MissingRelationshipDataException(
                    fieldType, relationshipDataString() + " is missing " + fieldType + " field");
        }
    }

    private String relationshipDataString() {
        return format("start:%s (%s) type:%s end:%s (%s)", startId, startIdGroup, type, endId, endIdGroup);
    }

    @Override
    public void close() {
        super.close();
        typeCounts.close();
        monitor.relationshipsImported(relationshipCount);
        relationshipUpdateCursor.close();
        cursorContext.close();
        idMapper.close();
    }

    @Override
    void freeUnusedIds() {
        super.freeUnusedIds();
        relationshipIds.markUnusedIdsAsDeleted(cursorContext);
    }
}
