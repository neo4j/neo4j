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
package org.neo4j.concurrencytest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Threading;
import org.neo4j.test.extension.ThreadingExtension;
import org.neo4j.values.storable.Values;

@ImpermanentDbmsExtension
@ExtendWith(ThreadingExtension.class)
class ConstraintIndexConcurrencyTest {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private Threading threads;

    @Test
    void shouldNotAllowConcurrentViolationOfConstraint() throws Exception {
        // Given
        Label label = label("Foo");
        String propertyKey = "bar";
        String conflictingValue = "baz";
        String constraintName = "MyConstraint";

        // a constraint
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(constraintName)
                    .create();
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            int labelId = ktx.tokenRead().nodeLabel(label.name());
            int propertyKeyId = ktx.tokenRead().propertyKey(propertyKey);
            Read read = ktx.dataRead();
            try (NodeValueIndexCursor cursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                IndexDescriptor index = ktx.schemaRead().indexGetForName(constraintName);
                IndexReadSession indexSession = ktx.dataRead().indexReadSession(index);
                read.nodeIndexSeek(
                        ktx.queryContext(),
                        indexSession,
                        cursor,
                        unconstrained(),
                        PropertyIndexQuery.exact(
                                propertyKeyId,
                                "The value is irrelevant, we just want to perform some sort of lookup against this "
                                        + "index"));
            }
            // then let another thread come in and create a node
            threads.execute(
                            db -> {
                                try (Transaction transaction = db.beginTx()) {
                                    transaction.createNode(label).setProperty(propertyKey, conflictingValue);
                                    transaction.commit();
                                }
                                return null;
                            },
                            db)
                    .get();

            // before we create a node with the same property ourselves - using the same statement that we have
            // already used for lookup against that very same index
            long node = ktx.dataWrite().nodeCreate();
            ktx.dataWrite().nodeAddLabel(node, labelId);

            var e = assertThrows(UniquePropertyValueValidationException.class, () -> ktx.dataWrite()
                    .nodeSetProperty(node, propertyKeyId, Values.of(conflictingValue)));
            assertEquals(ConstraintDescriptorFactory.uniqueForLabel(labelId, propertyKeyId), e.constraint());
            IndexEntryConflictException conflict =
                    Iterators.single(e.conflicts().iterator());
            assertEquals(
                    Values.stringValue(conflictingValue),
                    conflict.getPropertyValues().getOnlyValue());

            tx.commit();
        }
    }
}
