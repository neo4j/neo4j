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
package org.neo4j.internal.kernel.api.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class UnionRelationshipTypeIndexCursorTest {
    @Inject
    private Kernel kernel;

    @Test
    void shouldHandleEmptyResultAscending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int otherType = tokenWrite.relationshipTypeGetOrCreateForName("OTHER");
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);

            for (int i = 0; i < relCount; i++) {
                write.relationshipCreate(write.nodeCreate(), otherType, write.nodeCreate());
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            assertThat(unionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldHandleEmptyResultDescending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int otherType = tokenWrite.relationshipTypeGetOrCreateForName("OTHER");
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);

            for (int i = 0; i < relCount; i++) {
                write.relationshipCreate(write.nodeCreate(), otherType, write.nodeCreate());
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            assertThat(unionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldHandlePartiallyEmptyResultAscending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        List<Long> relsToFind = new ArrayList<>(relCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);

            for (int i = 0; i < relCount; i++) {
                relsToFind.add(write.relationshipCreate(write.nodeCreate(), typesToLookFor[1], write.nodeCreate()));
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(relsToFind);
        }
    }

    @Test
    void shouldHandlePartiallyEmptyResultDescending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        List<Long> relsToFind = new ArrayList<>(relCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);

            for (int i = 0; i < relCount; i++) {
                relsToFind.add(write.relationshipCreate(write.nodeCreate(), typesToLookFor[1], write.nodeCreate()));
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            relsToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(relsToFind);
        }
    }

    @Test
    void shouldHandledRelationshipsWithNoOverlapAscending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        List<Long> relsToFind = new ArrayList<>(relCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);
            int notLookingFor = tx.tokenWrite().relationshipTypeGetOrCreateForName("D");

            for (int i = 0; i < relCount; i++) {
                long relToFind =
                        write.relationshipCreate(write.nodeCreate(), typesToLookFor[i % 3], write.nodeCreate());
                relsToFind.add(relToFind);
                write.relationshipCreate(write.nodeCreate(), notLookingFor, write.nodeCreate());
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).containsExactlyInAnyOrderElementsOf(relsToFind);
        }
    }

    @Test
    void shouldHandledRelationshipsWithNoOverlapDescending() throws KernelException {
        // given
        int[] typesToLookFor = new int[3];
        int relCount = 100;
        List<Long> relsToFind = new ArrayList<>(relCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.relationshipTypeGetOrCreateForNames(new String[] {"A", "B", "C"}, typesToLookFor);
            int notLookingFor = tx.tokenWrite().relationshipTypeGetOrCreateForName("D");

            for (int i = 0; i < relCount; i++) {
                long relToFind =
                        write.relationshipCreate(write.nodeCreate(), typesToLookFor[i % 3], write.nodeCreate());
                relsToFind.add(relToFind);
                write.relationshipCreate(write.nodeCreate(), notLookingFor, write.nodeCreate());
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            var cursors = new RelationshipTypeIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionRelationshipTypeIndexCursor(tx, typesToLookFor, cursors);

            // then
            relsToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(relsToFind);
        }
    }

    private UnionRelationshipTypeIndexCursor ascendingUnionRelationshipTypeIndexCursor(
            KernelTransaction tx, int[] typesToLookFor, RelationshipTypeIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        SchemaRead schemaRead = tx.schemaRead();
        IndexDescriptor index =
                schemaRead.index(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR).next();
        TokenReadSession tokenReadSession = read.tokenReadSession(index);
        return UnionRelationshipTypeIndexCursor.ascendingUnionRelationshipTypeIndexCursor(
                read, tokenReadSession, tx.cursorContext(), typesToLookFor, cursors);
    }

    private UnionRelationshipTypeIndexCursor descendingUnionRelationshipTypeIndexCursor(
            KernelTransaction tx, int[] typesToLookFor, RelationshipTypeIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        SchemaRead schemaRead = tx.schemaRead();
        IndexDescriptor index =
                schemaRead.index(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR).next();
        TokenReadSession tokenReadSession = read.tokenReadSession(index);
        return UnionRelationshipTypeIndexCursor.descendingUnionRelationshipTypeIndexCursor(
                read, tokenReadSession, tx.cursorContext(), typesToLookFor, cursors);
    }

    private List<Long> asList(UnionRelationshipTypeIndexCursor cursor) {
        var result = new ArrayList<Long>();
        while (cursor.next()) {
            result.add(cursor.reference());
        }
        return result;
    }
}
