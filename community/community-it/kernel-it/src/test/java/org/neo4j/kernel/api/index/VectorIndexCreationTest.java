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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Tags;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class VectorIndexCreationTest {
    private static final Label LABEL = Tags.Suppliers.UUID.LABEL.get();
    private static final RelationshipType REL_TYPE = Tags.Suppliers.UUID.RELATIONSHIP_TYPE.get();
    private static final List<String> PROP_KEYS = Tags.Suppliers.UUID.PROPERTY_KEY.get(2);

    @Inject
    private GraphDatabaseAPI db;

    private int labelId;
    private int relTypeId;
    private int[] propKeyIds;

    @BeforeEach
    void setup() throws Exception {
        try (final var tx = db.beginTx()) {
            final var ktx = ((InternalTransaction) tx).kernelTransaction();
            labelId = Tags.Factories.LABEL.getId(ktx, LABEL);
            relTypeId = Tags.Factories.RELATIONSHIP_TYPE.getId(ktx, REL_TYPE);
            propKeyIds = Tags.Factories.PROPERTY_KEY.getIds(ktx, PROP_KEYS).stream()
                    .mapToInt(k -> k)
                    .toArray();
            tx.commit();
        }
    }

    @Test
    void shouldAcceptNodeVectorIndex() {
        assertThatCode(() -> createVectorIndex(SchemaDescriptors.forLabel(labelId, propKeyIds[0])))
                .doesNotThrowAnyException();
        assertThatCode(() -> createNodeVectorIndexCoreAPI(PROP_KEYS.get(1))).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectRelationshipVectorIndex() {
        assertUnsupportedRelationshipIndex(
                () -> createVectorIndex(SchemaDescriptors.forRelType(relTypeId, propKeyIds[0])));
        assertUnsupportedRelationshipIndex(() -> createRelVectorIndexCoreAPI(PROP_KEYS.get(1)));
    }

    @Test
    void shouldRejectCompositeKeys() {
        assertUnsupportedComposite(() -> createVectorIndex(SchemaDescriptors.forLabel(labelId, propKeyIds)));
        assertUnsupportedComposite(() -> createVectorIndex(SchemaDescriptors.forRelType(relTypeId, propKeyIds)));
        assertUnsupportedComposite(() -> createNodeVectorIndexCoreAPI(PROP_KEYS));
        assertUnsupportedComposite(() -> createRelVectorIndexCoreAPI(PROP_KEYS));
    }

    private void createVectorIndex(SchemaDescriptor schema) throws Exception {
        try (final var tx = db.beginTx()) {
            final var ktx = ((InternalTransaction) tx).kernelTransaction();
            final var prototype = IndexPrototype.forSchema(schema)
                    .withIndexType(IndexType.VECTOR)
                    .withIndexConfig(IndexSettingUtil.defaultConfigForTest(IndexType.VECTOR.toPublicApi()));
            ktx.schemaWrite().indexCreate(prototype);
            tx.commit();
        }
    }

    private void createNodeVectorIndexCoreAPI(String... propKeys) {
        createNodeVectorIndexCoreAPI(Arrays.asList(propKeys));
    }

    private void createNodeVectorIndexCoreAPI(List<String> propKeys) {
        try (final var tx = db.beginTx()) {
            var creator = tx.schema()
                    .indexFor(LABEL)
                    .withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi()));
            for (final var propKey : propKeys) {
                creator = creator.on(propKey);
            }
            creator.create();
            tx.commit();
        }
    }

    private void createRelVectorIndexCoreAPI(String... propKeys) {
        createRelVectorIndexCoreAPI(Arrays.asList(propKeys));
    }

    private void createRelVectorIndexCoreAPI(List<String> propKeys) {
        try (final var tx = db.beginTx()) {
            var creator = tx.schema()
                    .indexFor(REL_TYPE)
                    .withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(IndexType.VECTOR.toPublicApi()));
            for (final var propKey : propKeys) {
                creator = creator.on(propKey);
            }
            creator.create();
            tx.commit();
        }
    }

    private static void assertUnsupportedRelationshipIndex(ThrowingCallable callable) {
        assertUnsupported(callable)
                .hasMessageContainingAll(
                        "Relationship indexes are not supported for", IndexType.VECTOR.name(), "index type");
    }

    private static void assertUnsupportedComposite(ThrowingCallable callable) {
        assertUnsupported(callable)
                .hasMessageContainingAll(
                        "Composite indexes are not supported for", IndexType.VECTOR.name(), "index type");
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertUnsupported(ThrowingCallable callable) {
        return assertThatThrownBy(callable).isInstanceOf(UnsupportedOperationException.class);
    }
}
