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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestSupport;
import org.neo4j.memory.EmptyMemoryTracker;

public abstract class AbstractIndexQueryingTest<S extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<S> {
    @Override
    public void createTestGraph(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.execute("CREATE FULLTEXT INDEX ftsNodes FOR (n:Label) ON EACH [n.prop]")
                    .close();
            tx.execute("CREATE FULLTEXT INDEX ftsRels FOR ()-[r:Type]-() ON EACH [r.prop]")
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
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName("ftsRels"));
        try (NodeValueIndexCursor cursor =
                cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.nodeIndexSeek(
                            tx.queryContext(),
                            index,
                            cursor,
                            unconstrained(),
                            PropertyIndexQuery.fulltextSearch("search")));
        }
    }

    @Test
    void relationshipIndexSeekMustThrowOnWrongIndexEntityType() throws IndexNotFoundKernelException {
        IndexDescriptor index = schemaRead.indexGetForName("ftsNodes");
        IndexReadSession indexReadSession = read.indexReadSession(index);
        try (RelationshipValueIndexCursor cursor =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> read.relationshipIndexSeek(
                            tx.queryContext(),
                            indexReadSession,
                            cursor,
                            unconstrained(),
                            PropertyIndexQuery.fulltextSearch("search")));
        }
    }
}
