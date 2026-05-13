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
package org.neo4j.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class IndexQueryingTest extends KernelAPIReadTestBase<ReadTestSupport> {
    private static final String NODE_INDEX_NAME = "ftsNodes";
    private static final String REL_INDEX_NAME = "ftsRels";

    protected final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport() {
            @Override
            protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder(Path storeDir) {
                return super.newManagementServiceBuilder(storeDir)
                        .setInternalLogProvider(logProvider)
                        .useLazyProcedures(false);
            }
        };
    }

    @Override
    public void createTestGraph(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.execute("CREATE FULLTEXT INDEX %s FOR (n:Label) ON EACH [n.prop]".formatted(NODE_INDEX_NAME))
                    .close();
            tx.execute("CREATE FULLTEXT INDEX %s FOR ()-[r:Type]-() ON EACH [r.prop]".formatted(REL_INDEX_NAME))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    @Test
    void nodeIndexSeekMustThrowOnWrongIndexEntityType() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(REL_INDEX_NAME));
        try (NodeValueIndexCursor cursor =
                cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.nodeIndexSeek(
                            tx.queryContext(),
                            index,
                            cursor,
                            unconstrained(),
                            PropertyIndexQuery.fulltextSearch("search")));
            assertThat(e).hasMessageContaining("Node index seek can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                    .formatted(REL_INDEX_NAME));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Node index seek can not be performed on index")
                    .containsException(e);
        }
    }

    @Test
    void partitionedNodeIndexSeekMustThrowOnWrongIndexEntityType() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(REL_INDEX_NAME));
        IndexNotApplicableKernelException e = assertThrows(
                IndexNotApplicableKernelException.class,
                () -> read.nodeIndexSeek(index, 10, tx.queryContext(), PropertyIndexQuery.fulltextSearch("search")));
        assertThat(e).hasMessageContaining("Node index seek can not be performed on index");
        assertThat(e.gqlStatus()).isEqualTo("50N15");
        assertThat(e.statusDescription())
                .isEqualTo(
                        "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                .formatted(REL_INDEX_NAME));
        LogAssertions.assertThat(logProvider)
                .containsMessageWithAll("Node index seek can not be performed on index")
                .containsException(e);
    }

    @Test
    void relationshipIndexSeekMustThrowOnWrongIndexEntityType() throws IndexNotFoundKernelException {
        IndexDescriptor index = schemaRead.indexGetForName(NODE_INDEX_NAME);
        IndexReadSession indexReadSession = read.indexReadSession(index);
        try (RelationshipValueIndexCursor cursor =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.relationshipIndexSeek(
                            tx.queryContext(),
                            indexReadSession,
                            cursor,
                            unconstrained(),
                            PropertyIndexQuery.fulltextSearch("search")));
            assertThat(e).hasMessageContaining("Relationship index seek can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                    .formatted(NODE_INDEX_NAME));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Relationship index seek can not be performed on index")
                    .containsException(e);
        }
    }

    @Test
    void partitionedRelationshipIndexSeekMustThrowOnWrongIndexEntityType() throws IndexNotFoundKernelException {
        IndexDescriptor index = schemaRead.indexGetForName(NODE_INDEX_NAME);
        IndexReadSession indexReadSession = read.indexReadSession(index);
        IndexNotApplicableKernelException e = assertThrows(
                IndexNotApplicableKernelException.class,
                () -> read.relationshipIndexSeek(
                        indexReadSession, 10, tx.queryContext(), PropertyIndexQuery.fulltextSearch("search")));
        assertThat(e).hasMessageContaining("Relationship index seek can not be performed on index");
        assertThat(e.gqlStatus()).isEqualTo("50N15");
        assertThat(e.statusDescription())
                .isEqualTo(
                        "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                .formatted(NODE_INDEX_NAME));
        LogAssertions.assertThat(logProvider)
                .containsMessageWithAll("Relationship index seek can not be performed on index")
                .containsException(e);
    }

    @Test
    void nodeIndexScanMustThrowOnWrongIndexEntityType() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(REL_INDEX_NAME));
        try (NodeValueIndexCursor cursor =
                cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class, () -> read.nodeIndexScan(index, cursor, unconstrained()));
            assertThat(e).hasMessageContaining("Node index scan can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                    .formatted(REL_INDEX_NAME));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Node index scan can not be performed on index")
                    .containsException(e);
        }
    }

    @Test
    void relationshipIndexScanMustThrowOnWrongIndexEntityType() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(NODE_INDEX_NAME));
        try (RelationshipValueIndexCursor cursor =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.relationshipIndexScan(index, cursor, unconstrained()));
            assertThat(e).hasMessageContaining("Relationship index scan can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information."
                                    .formatted(NODE_INDEX_NAME));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Relationship index scan can not be performed on index")
                    .containsException(e);
        }
    }

    @Test
    void nodeLabelIndexScanMustThrowOnWrongIndexEntityType() throws Exception {
        TokenReadSession tokenReadSession = read.tokenReadSession(schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR)
                .next());
        try (NodeLabelIndexCursor cursor =
                cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.nodeLabelScan(
                            tokenReadSession, cursor, unconstrained(), new TokenPredicate(1), tx.cursorContext()));
            assertThat(e).hasMessageContaining("Node label index scan can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .contains(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index",
                            "See debug.log for more information.");
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Node label index scan can not be performed on index")
                    .containsException(e);
        }
    }

    @Test
    void relationshipTypeIndexScanMustThrowOnWrongIndexEntityType() throws Exception {
        TokenReadSession tokenReadSession = read.tokenReadSession(schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        try (RelationshipTypeIndexCursor cursor =
                cursors.allocateRelationshipTypeIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.relationshipTypeScan(
                            tokenReadSession, cursor, unconstrained(), new TokenPredicate(1), tx.cursorContext()));
            assertThat(e).hasMessageContaining("Relationship type index scan can not be performed on index");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .contains(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index",
                            "See debug.log for more information.");
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Relationship type index scan can not be performed on index")
                    .containsException(e);
        }
    }
}
