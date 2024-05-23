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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.schema.IndexType.RANGE;
import static org.neo4j.internal.schema.IndexType.TEXT;
import static org.neo4j.internal.schema.SchemaRuleMapifier.mapifySchemaRule;
import static org.neo4j.internal.schema.SchemaRuleMapifier.unmapifySchemaRule;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.keyForSchema;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.RandomSchema;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SchemaStoreMapificationTest {
    private static final RandomSchema RANDOM_SCHEMA = new RandomSchema();

    private final LabelSchemaDescriptor labelSchema = SchemaDescriptors.forLabel(1, 2, 3);
    private final RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptors.forRelType(1, 2, 3);
    private final FulltextSchemaDescriptor fulltextNodeSchema =
            SchemaDescriptors.fulltext(NODE, new int[] {1, 2}, new int[] {1, 2});
    private final FulltextSchemaDescriptor fulltextRelSchema =
            SchemaDescriptors.fulltext(RELATIONSHIP, new int[] {1, 2}, new int[] {1, 2});
    private final IndexProviderDescriptor tree = new IndexProviderDescriptor("range", "1.0");
    private final IndexProviderDescriptor fts = new IndexProviderDescriptor("fulltext", "1.0");
    private final IndexDescriptor labelIndex =
            forSchema(labelSchema, tree).withName("labelIndex").materialise(1);
    private final IndexDescriptor labelUniqueIndex =
            uniqueForSchema(labelSchema, tree).withName("labelUniqueIndex").materialise(1);
    private final IndexDescriptor relTypeIndex =
            forSchema(relTypeSchema, tree).withName("relTypeIndex").materialise(1);
    private final IndexDescriptor relTypeUniqueIndex =
            uniqueForSchema(relTypeSchema, tree).withName("relTypeUniqueIndex").materialise(1);
    private final IndexDescriptor nodeFtsIndex = forSchema(fulltextNodeSchema, fts)
            .withIndexType(FULLTEXT)
            .withName("nodeFtsIndex")
            .materialise(1);
    private final IndexDescriptor relFtsIndex = forSchema(fulltextRelSchema, fts)
            .withIndexType(FULLTEXT)
            .withName("relFtsIndex")
            .materialise(1);
    // Text is not an index type we support for constraints but let's see that we can actually store/read it correctly
    private final ConstraintDescriptor uniqueLabelConstraint = ConstraintDescriptorFactory.uniqueForSchema(
                    labelSchema, TEXT)
            .withName("uniqueLabelConstraint")
            .withId(1);
    private final ConstraintDescriptor nodeKeyConstraint =
            keyForSchema(labelSchema, TEXT).withName("nodeKeyConstraint").withId(1);
    private final ConstraintDescriptor uniqueLabelConstraintWithType = ConstraintDescriptorFactory.uniqueForSchema(
                    labelSchema, RANGE)
            .withName("uniqueLabelConstraintWithType")
            .withOwnedIndexId(7)
            .withId(1);
    private final ConstraintDescriptor existsLabelConstraint = existsForSchema(labelSchema, false)
            .withName("existsLabelConstraint")
            .withId(1);
    private final ConstraintDescriptor nodeKeyConstraintWithType = keyForSchema(labelSchema, RANGE)
            .withName("nodeKeyConstraintWithType")
            .withOwnedIndexId(7)
            .withId(1);
    private final ConstraintDescriptor existsRelTypeConstraint = existsForSchema(relTypeSchema, false)
            .withName("existsRelTypeConstraint")
            .withId(1);
    private final ConstraintDescriptor relKeyConstraint =
            keyForSchema(relTypeSchema, TEXT).withName("relKeyConstraint").withId(1);
    private final ConstraintDescriptor relKeyConstraintWithType = keyForSchema(relTypeSchema, RANGE)
            .withName("relKeyConstraintWithType")
            .withOwnedIndexId(7)
            .withId(1);
    private final ConstraintDescriptor uniqueRelTypeConstraint = ConstraintDescriptorFactory.uniqueForSchema(
                    relTypeSchema, TEXT)
            .withName("uniqueRelTypeConstraint")
            .withId(1);
    private final ConstraintDescriptor uniqueRelTypeConstraintWithType = ConstraintDescriptorFactory.uniqueForSchema(
                    relTypeSchema, RANGE)
            .withName("uniqueRelTypeConstraintWithType")
            .withOwnedIndexId(7)
            .withId(1);
    private final ConstraintDescriptor nodeTypeConstraintSingleScalarType = ConstraintDescriptorFactory.typeForSchema(
                    labelSchema, PropertyTypeSet.of(SchemaValueType.BOOLEAN), false)
            .withName("nodeTypeConstrainSingleScalarType")
            .withId(1);
    private final ConstraintDescriptor relTypeConstraintSeveralTypes = ConstraintDescriptorFactory.typeForSchema(
                    relTypeSchema,
                    PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.LOCAL_DATETIME, SchemaValueType.FLOAT),
                    false)
            .withName("relTypeConstrainSeveralTypes")
            .withId(1);

    @RepeatedTest(500)
    void mapificationMustPreserveSchemaRulesAccurately() throws MalformedSchemaRuleException {
        SchemaRule rule = RANDOM_SCHEMA.get();
        Map<String, Value> mapified = mapifySchemaRule(rule);
        SchemaRule unmapified = unmapifySchemaRule(rule.getId(), mapified);
        if (!rule.equals(unmapified) || !unmapified.equals(rule)) {
            fail("Mapification of schema rule was not fully reversible.\n" + "Expected: "
                    + rule + "\n" + "But got:  "
                    + unmapified + "\n" + "Mapified rule: "
                    + mapified);
        }
    }

    @Test
    void labelIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'labelIndex', RANGE, :label[1](property[2], property[3]), range-1.0 )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("labelIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("range"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("NON_UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(labelIndex);
    }

    @Test
    void labelUniqueIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'labelUniqueIndex', RANGE, :label[1](property[2], property[3]), range-1.0 )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("labelUniqueIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("range"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(labelUniqueIndex);
    }

    @Test
    void relTypeIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'relTypeIndex', RANGE, -[:relType[1](property[2], property[3])]-, range-1.0 )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relTypeIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("range"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("NON_UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relTypeIndex);
    }

    @Test
    void relTypeUniqueIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'relTypeUniqueIndex', RANGE, -[:relType[1](property[2], property[3])]-, range-1.0 )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relTypeUniqueIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("range"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relTypeUniqueIndex);
    }

    @Test
    void nodeFtsIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'nodeFtsIndex', FULLTEXT, :label[1],label[2](property[1], property[2]), fulltext-1.0 )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("nodeFtsIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("PARTIAL_ANY_TOKEN"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("fulltext"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {1, 2}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("FULLTEXT"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("NON_UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1, 2}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(nodeFtsIndex);
    }

    @Test
    void relFtsIndexDeterministicUnmapification() throws Exception {
        // Index( 1, 'relFtsIndex', FULLTEXT, -[:relType[1],relType[2](property[1], property[2])]-, fulltext-1.0
        // )
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relFtsIndex"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("PARTIAL_ANY_TOKEN"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderName", Values.stringValue("fulltext"));
        mapified.put("__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue("1.0"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {1, 2}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("INDEX"));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("FULLTEXT"));
        mapified.put("__org.neo4j.SchemaRule.indexRuleType", Values.stringValue("NON_UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1, 2}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relFtsIndex);
    }

    @Test
    void setUniqueLabelConstraintWithOtherTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("uniqueLabelConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("TEXT"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(uniqueLabelConstraint);
    }

    @Test
    void setUniqueLabelConstraintWithTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("uniqueLabelConstraintWithType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.ownedIndexId", Values.longValue(7));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(uniqueLabelConstraintWithType);
    }

    @Test
    void setUniqueRelTypeConstraintWithOtherTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("uniqueRelTypeConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("TEXT"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(uniqueRelTypeConstraint);
    }

    @Test
    void setUniqueRelTypeConstraintWithTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("uniqueRelTypeConstraintWithType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.ownedIndexId", Values.longValue(7));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(uniqueRelTypeConstraintWithType);
    }

    @Test
    void existsLabelConstraintDeterministicUnmapification() throws Exception {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@41402017
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("existsLabelConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(existsLabelConstraint);
    }

    @Test
    void nodeKeyConstraintWithOtherTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("nodeKeyConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE_EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("TEXT"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(nodeKeyConstraint);
    }

    @Test
    void nodeKeyConstraintWithTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("nodeKeyConstraintWithType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE_EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.ownedIndexId", Values.longValue(7));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(nodeKeyConstraintWithType);
    }

    @Test
    void relKeyConstraintWithOtherTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relKeyConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE_EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("TEXT"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relKeyConstraint);
    }

    @Test
    void relKeyConstraintWithTypeDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relKeyConstraintWithType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE_EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.ownedIndexId", Values.longValue(7));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("UNDESIGNATED"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relKeyConstraintWithType);
    }

    @Test
    void relKeyConstraintWithTypeDeterministicUnmapificationNoGraphTypeDependence() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relKeyConstraintWithType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("UNIQUE_EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.indexType", Values.stringValue("RANGE"));
        mapified.put("__org.neo4j.SchemaRule.ownedIndexId", Values.longValue(7));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relKeyConstraintWithType);
    }

    @Test
    void existsRelTypeConstraintDeterministicUnmapification() throws Exception {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@40083801
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("existsRelTypeConstraint"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("EXISTS"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(existsRelTypeConstraint);
    }

    @Test
    void propTypeLabelConstraintDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("NODE"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("nodeTypeConstraintSingleScalarType"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("PROPERTY_TYPE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.propertyType", Values.stringArray("BOOLEAN"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(nodeTypeConstraintSingleScalarType);
    }

    @Test
    void propTypeRelTypeConstraintDeterministicUnmapification() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relTypeConstrainSeveralTypes"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("PROPERTY_TYPE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.propertyType", Values.stringArray("BOOLEAN", "LOCAL_DATETIME", "FLOAT"));
        mapified.put("__org.neo4j.SchemaRule.graphTypeDependence", Values.stringValue("INDEPENDENT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relTypeConstraintSeveralTypes);
    }

    @Test
    void propTypeRelTypeConstraintDeterministicUnmapificationNoGraphTypeDependence() throws Exception {
        Map<String, Value> mapified = new HashMap<>();

        mapified.put("__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue("RELATIONSHIP"));
        mapified.put("__org.neo4j.SchemaRule.name", Values.stringValue("relTypeConstrainSeveralTypes"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue("COMPLETE_ALL_TOKENS"));
        mapified.put("__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray(new int[] {2, 3}));
        mapified.put("__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue("CONSTRAINT"));
        mapified.put("__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue("PROPERTY_TYPE"));
        mapified.put("__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray(new int[] {1}));
        mapified.put("__org.neo4j.SchemaRule.propertyType", Values.stringArray("BOOLEAN", "LOCAL_DATETIME", "FLOAT"));

        assertThat(unmapifySchemaRule(1, mapified)).isEqualTo(relTypeConstraintSeveralTypes);
    }
}
