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
package org.neo4j.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class IndexDescriptorTest {
    private static final SchemaDescriptor[] SCHEMAS = {
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1}, new int[] {2}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {2}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1, 1}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1, 1}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1, 2}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1, 1}, new int[] {1, 2}),
        SchemaDescriptors.forSemanticSearch(EntityType.NODE, new int[] {1, 2}, new int[] {1, 2}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1}, new int[] {2}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {2}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1, 1}, new int[] {1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1, 1}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1, 2}, new int[] {1, 1}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1, 1}, new int[] {1, 2}),
        SchemaDescriptors.forSemanticSearch(EntityType.RELATIONSHIP, new int[] {1, 2}, new int[] {1, 2}),
        SchemaDescriptors.forLabel(1, 1),
        SchemaDescriptors.forLabel(1, 2),
        SchemaDescriptors.forLabel(2, 1),
        SchemaDescriptors.forLabel(2, 2),
        SchemaDescriptors.forLabel(1, 1, 1),
        SchemaDescriptors.forLabel(1, 1, 2),
        SchemaDescriptors.forLabel(1, 2, 1),
        SchemaDescriptors.forLabel(1, 2, 2),
        SchemaDescriptors.forLabel(2, 1, 1),
        SchemaDescriptors.forLabel(2, 2, 1),
        SchemaDescriptors.forLabel(2, 1, 2),
        SchemaDescriptors.forLabel(2, 2, 2),
        SchemaDescriptors.forRelType(1, 1),
        SchemaDescriptors.forRelType(1, 2),
        SchemaDescriptors.forRelType(2, 1),
        SchemaDescriptors.forRelType(2, 2),
        SchemaDescriptors.forRelType(1, 1, 1),
        SchemaDescriptors.forRelType(1, 1, 2),
        SchemaDescriptors.forRelType(1, 2, 1),
        SchemaDescriptors.forRelType(1, 2, 2),
        SchemaDescriptors.forRelType(2, 1, 1),
        SchemaDescriptors.forRelType(2, 2, 1),
        SchemaDescriptors.forRelType(2, 1, 2),
        SchemaDescriptors.forRelType(2, 2, 2),
        SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR,
        SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR
    };

    @Test
    void indexDescriptorsMustBeDistinctBySchema() {
        List<IndexDescriptor> indexes = Stream.of(SCHEMAS)
                .flatMap(IndexDescriptorTest::validPrototypesFor)
                .map(prototype -> prototype.withName("index").materialise(0))
                .toList();

        Set<IndexDescriptor> indexSet = new HashSet<>();
        for (IndexDescriptor index : indexes) {
            if (!indexSet.add(index)) {
                IndexDescriptor existing = null;
                for (IndexDescriptor candidate : indexSet) {
                    if (candidate.equals(index)) {
                        existing = candidate;
                        break;
                    }
                }
                fail("Index descriptor equality collision: " + existing + " and " + index);
            }
        }
    }

    @Test
    void mustThrowWhenCreatingIndexNamedAfterNoIndexName() {
        assertThatThrownBy(() -> {
                    IndexPrototype prototype = IndexPrototype.forSchema(SCHEMAS[0]);
                    prototype = prototype.withName(IndexDescriptor.NO_INDEX.getName());
                    prototype.materialise(0);
                })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IndexDescriptor.NO_INDEX.getName());
    }

    @Test
    void userDescriptionMustIncludeSchemaDescription() {
        IndexPrototype prototype = IndexPrototype.forSchema(SCHEMAS[0]);
        IndexDescriptor index = prototype.withName("index").materialise(1);
        InMemoryTokens tokens = new InMemoryTokens();
        String schemaDescription = SCHEMAS[0].userDescription(tokens);
        assertThat(prototype.userDescription(tokens)).contains(schemaDescription);
        assertThat(index.userDescription(tokens)).contains(schemaDescription);
    }

    @Test
    void updatingIndexConfigLeavesOriginalDescriptorUntouched() {
        IndexDescriptor a = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("a")
                .materialise(1);
        IndexDescriptor aa = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("a")
                .materialise(1);
        IndexDescriptor b = a.withIndexConfig(a.getIndexConfig().withIfAbsent("x", Values.stringValue("y")));

        assertThat(a.getIndexConfig()).isNotEqualTo(b.getIndexConfig());
        assertThat(a).isEqualTo(b).isEqualTo(aa);
        assertThat(a.getIndexConfig()).isEqualTo(aa.getIndexConfig());
        assertThat((Value) b.getIndexConfig().get("x")).isEqualTo(Values.stringValue("y"));
        assertThat((Value) a.getIndexConfig().get("x")).isNull();
    }

    private static Stream<IndexPrototype> validPrototypesFor(SchemaDescriptor schema) {
        int[] entityTokenIds = schema.getEntityTokenIds();
        if (IntSets.mutable.of(entityTokenIds).size() != entityTokenIds.length) {
            return Stream.empty();
        }

        int[] propertyKeyIds = schema.getPropertyIds();
        if (IntSets.mutable.of(propertyKeyIds).size() != propertyKeyIds.length) {
            return Stream.empty();
        }

        Stream.Builder<IndexPrototype> prototypes = Stream.builder();
        IndexPrototype unique = IndexPrototype.uniqueForSchema(schema);
        IndexPrototype nonUnique = IndexPrototype.forSchema(schema);
        if (schema.isAnyTokenSchemaDescriptor()) {
            prototypes.add(nonUnique.withIndexType(IndexType.LOOKUP));
        } else if (schema.isSemanticSearchSchemaDescriptor()) {
            prototypes.add(nonUnique.withIndexType(IndexType.FULLTEXT));
            prototypes.add(nonUnique.withIndexType(IndexType.VECTOR));
        } else if (schema.getPropertyIds().length > 1) {
            prototypes.add(unique.withIndexType(IndexType.RANGE));
            prototypes.add(nonUnique.withIndexType(IndexType.RANGE));
        } else {
            prototypes.add(unique.withIndexType(IndexType.RANGE));
            prototypes.add(nonUnique.withIndexType(IndexType.RANGE));
            prototypes.add(nonUnique.withIndexType(IndexType.POINT));
        }
        return prototypes.build();
    }
}
