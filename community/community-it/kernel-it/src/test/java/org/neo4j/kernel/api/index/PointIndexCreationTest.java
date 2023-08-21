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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class PointIndexCreationTest {
    @Inject
    private GraphDatabaseAPI db;

    private int labelId;
    private int relTypeId;
    private int[] compositeKey;

    @BeforeEach
    void setup() throws Exception {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenWrite tokenWrite = ktx.tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName(label("PERSON").name());
            relTypeId = tokenWrite.relationshipTypeGetOrCreateForName(
                    RelationshipType.withName("FRIEND").name());
            compositeKey = new int[] {
                tokenWrite.propertyKeyGetOrCreateForName("address"), tokenWrite.propertyKeyGetOrCreateForName("age")
            };
            tx.commit();
        }
    }

    @Test
    void shouldRejectCompositeKeys() {
        assertUnsupported(() -> createPointIndex("ni", SchemaDescriptors.forLabel(labelId, compositeKey)));
        assertUnsupported(() -> createPointIndex("ri", SchemaDescriptors.forRelType(relTypeId, compositeKey)));
        assertUnsupported(this::createCompositeNodePointIndexCoreAPI);
        assertUnsupported(this::createCompositeRelPointIndexCoreAPI);
    }

    private void assertUnsupported(Executable executable) {
        var message =
                assertThrows(UnsupportedOperationException.class, executable).getMessage();
        assertThat(message).isEqualTo("Composite indexes are not supported for POINT index type.");
    }

    private void createPointIndex(String name, SchemaDescriptor schema) throws Exception {
        try (Transaction tx = db.beginTx()) {
            var prototype = IndexPrototype.forSchema(schema)
                    .withIndexType(IndexType.POINT)
                    .withName(name);
            var kernelTransaction = ((InternalTransaction) tx).kernelTransaction();
            kernelTransaction.schemaWrite().indexCreate(prototype);
            tx.commit();
        }
    }

    private void createCompositeNodePointIndexCoreAPI() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(label("label"))
                    .on("prop")
                    .on("prop2")
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.POINT)
                    .create();
            tx.commit();
        }
    }

    private void createCompositeRelPointIndexCoreAPI() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(RelationshipType.withName("type"))
                    .on("prop")
                    .on("prop2")
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.POINT)
                    .create();
            tx.commit();
        }
    }
}
