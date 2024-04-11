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
package org.neo4j.kernel.impl.store.stats;

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import org.neo4j.counts.CountsStore;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.pagecache.context.CursorContext;

public class RecordDatabaseEntityCounters implements StoreEntityCounters {
    private final IdGeneratorFactory idGeneratorFactory;
    private final CountsStore countsStore;

    public RecordDatabaseEntityCounters(IdGeneratorFactory idGeneratorFactory, CountsStore countsStore) {
        this.idGeneratorFactory = idGeneratorFactory;
        this.countsStore = countsStore;
    }

    @Override
    public long nodes(CursorContext cursorContext) {
        return idGeneratorFactory.get(RecordIdType.NODE).getHighId();
    }

    @Override
    public long relationships(CursorContext cursorContext) {
        return idGeneratorFactory.get(RecordIdType.RELATIONSHIP).getHighId();
    }

    @Override
    public long properties(CursorContext cursorContext) {
        return idGeneratorFactory.get(RecordIdType.PROPERTY).getHighId();
    }

    @Override
    public long relationshipTypes(CursorContext cursorContext) {
        return idGeneratorFactory.get(SchemaIdType.RELATIONSHIP_TYPE_TOKEN).getHighId();
    }

    @Override
    public long allNodesCountStore(CursorContext cursorContext) {
        return countsStore.nodeCount(ANY_LABEL, cursorContext);
    }

    @Override
    public long allRelationshipsCountStore(CursorContext cursorContext) {
        return countsStore.relationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, cursorContext);
    }

    @Override
    public long estimateNodes() {
        return idGeneratorFactory.get(RecordIdType.NODE).getHighId();
    }

    @Override
    public long estimateRelationships() {
        return idGeneratorFactory.get(RecordIdType.RELATIONSHIP).getHighId();
    }

    @Override
    public long estimateLabels() {
        return idGeneratorFactory.get(SchemaIdType.LABEL_TOKEN).getHighId();
    }
}
