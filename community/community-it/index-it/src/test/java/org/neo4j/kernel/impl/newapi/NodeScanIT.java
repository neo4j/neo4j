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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.kernel.api.security.AnonymousContext.read;

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class NodeScanIT {
    @Inject
    private Kernel kernel;

    @Test
    void trackPageCacheAccessOnNodeLabelScan() throws KernelException {
        var testLabel = Label.label("testLabel");
        try (KernelTransaction tx = kernel.beginTransaction(IMPLICIT, read())) {
            var cursorContext = tx.cursorContext();
            assertThat(cursorContext.getCursorTracer().pins()).isZero();

            var label = tx.tokenRead().nodeLabel(testLabel.name());

            IndexDescriptor index = tx.schemaRead()
                    .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                    .next();
            TokenReadSession tokenReadSession = tx.dataRead().tokenReadSession(index);
            try (NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(cursorContext)) {
                tx.dataRead()
                        .nodeLabelScan(
                                tokenReadSession,
                                cursor,
                                IndexQueryConstraints.unconstrained(),
                                new TokenPredicate(label),
                                cursorContext);

                assertThat(cursorContext.getCursorTracer().pins()).isNotZero();
            }
        }
    }
}
