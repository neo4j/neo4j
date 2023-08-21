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
package org.neo4j.kernel.impl.newapi.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.memory.EmptyMemoryTracker;

public class RelationshipValueIndexCursorRange10Test
        extends EntityValueIndexCursorTestBase<RelationshipValueIndexCursor> {

    @Override
    protected IndexParams getIndexParams() {
        return new Range10IndexParams();
    }

    @Override
    protected EntityParams<RelationshipValueIndexCursor> getEntityParams() {
        return new RelationshipParams();
    }

    @Override
    protected IndexType getIndexType() {
        return IndexType.RANGE;
    }

    @Test
    void shouldReadRelationshipOnReadFromStore() throws Exception {
        // given
        var needsValues = indexParams.indexProvidesStringValues();
        var constraints = unordered(needsValues);
        var prop = token.propertyKey(PROP_NAME);
        var index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        long first;
        try (var tx = beginTransaction()) {
            var tokenId = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
            first = entityParams.entityCreateNew(tx, tokenId);
            entityParams.entitySetProperty(tx, first, tx.tokenRead().propertyKey(PROP_NAME), "aaaaa");
            tx.commit();
        }

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            int relType;
            long relSourceNode;
            long relTargetNode;
            try (var tx = beginTransaction();
                    var relationshipScanCursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                // This should be the first one returned from the data set at hand
                tx.dataRead().singleRelationship(first, relationshipScanCursor);
                relationshipScanCursor.next();
                relType = relationshipScanCursor.type();
                relSourceNode = relationshipScanCursor.sourceNodeReference();
                relTargetNode = relationshipScanCursor.targetNodeReference();
            }

            // do the seek
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "a", true, "three", true));
            // and go to the first entry, which is the above created "aaaaa" entry
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.readFromStore()).isTrue();

            // then delete that relationship
            try (var tx = beginTransaction()) {
                entityParams.entityDelete(tx, first);
                tx.commit();
            }

            // then assert that its information was already read
            assertThat(cursor.type()).isEqualTo(relType);
            assertThat(cursor.sourceNodeReference()).isEqualTo(relSourceNode);
            assertThat(cursor.targetNodeReference()).isEqualTo(relTargetNode);

            // then assert the rest of the expected relationships for that seek
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strOne);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree1);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree2);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree3);
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldNotLoadDeletedRelationshipOnReadFromStore() throws Exception {
        // given
        var needsValues = indexParams.indexProvidesStringValues();
        var constraints = unordered(needsValues);
        var prop = token.propertyKey(PROP_NAME);
        var index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        long first;
        try (var tx = beginTransaction()) {
            var tokenId = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
            first = entityParams.entityCreateNew(tx, tokenId);
            entityParams.entitySetProperty(tx, first, tx.tokenRead().propertyKey(PROP_NAME), "aaaaa");
            tx.commit();
        }

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            // do the seek
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "a", true, "three", true));
            // and go to the first entry, which is the strOne
            assertThat(cursor.next()).isTrue();

            // then delete that relationship
            try (var tx = beginTransaction()) {
                entityParams.entityDelete(tx, first);
                tx.commit();
            }

            // then
            assertThat(cursor.readFromStore()).isFalse();

            // then assert the rest of the expected relationships for that seek
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strOne);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree1);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree2);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.relationshipReference()).isEqualTo(strThree3);
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldFailOnReadRelationshipBeforeReadFromStore() throws Exception {
        // given
        var needsValues = indexParams.indexProvidesStringValues();
        var constraints = unordered(needsValues);
        var prop = token.propertyKey(PROP_NAME);
        var index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var propertyCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            // when
            // do the seek
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", true, "three", true));
            // and go to the first entry, which is the strOne
            assertThat(cursor.next()).isTrue();

            // then
            assertThatThrownBy(() -> cursor.source(nodeCursor)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::sourceNodeReference).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> cursor.target(nodeCursor)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::targetNodeReference).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::type).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> cursor.properties(propertyCursor)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::propertiesReference).isInstanceOf(IllegalStateException.class);
            assertThat(cursor.relationshipReference()).isEqualTo(strOne);
        }
    }
}
