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
package org.neo4j.kernel.api.exceptions.index;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

class IndexEntryConflictExceptionTest {
    private static final int labelId = 1;
    private static final int typeId = 1;
    private static final Value value = Values.of("hi");
    private static final InMemoryTokens tokens = new InMemoryTokens()
            .label(labelId, "label1")
            .relationshipType(typeId, "type1")
            .propertyKey(2, "p2")
            .propertyKey(3, "p3")
            .propertyKey(4, "p4");

    @Test
    void shouldMakeEntryConflicts() {
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, 2);
        IndexEntryConflictException e = new IndexEntryConflictException(schema, 0L, 1L, value);

        assertThat(e)
                .hasMessage("Both Node(0) and Node(1) have the label `Label[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("Both Node(0) and Node(1) have the label `label1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeEntryConflictsForOneNode() {
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, 2);
        IndexEntryConflictException e =
                new IndexEntryConflictException(schema, 0L, StatementConstants.NO_SUCH_NODE, value);

        assertThat(e).hasMessage("Node(0) already exists with label `Label[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("Node(0) already exists with label `label1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeAnonymousEntryConflictsForNewNodeUnknown() {
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, 2);
        IndexEntryConflictException e = new IndexEntryConflictException(
                schema, StatementConstants.NO_SUCH_NODE, StatementConstants.NO_SUCH_NODE, value);

        assertThat(e).hasMessage("A Node already exists with label `Label[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("A Node already exists with label `label1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeAnonymousEntryConflictsForNewNodeKnown() {
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, 2);
        IndexEntryConflictException e =
                new IndexEntryConflictException(schema, StatementConstants.NO_SUCH_NODE, 0L, value);

        assertThat(e)
                .hasMessage(
                        "Both another Node and Node(0) have the label `Label[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("Both another Node and Node(0) have the label `label1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeCompositeEntryConflicts() {
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, 2, 3, 4);
        ValueTuple values = ValueTuple.of(true, "hi", new long[] {6L, 4L});
        IndexEntryConflictException e = new IndexEntryConflictException(schema, 0L, 1L, values);

        assertThat(e)
                .hasMessage(
                        "Both Node(0) and Node(1) have the label `Label[1]` and properties `PropertyKey[2]` = true, `PropertyKey[3]` = 'hi', `PropertyKey[4]` = [6, 4]");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo(
                        "Both Node(0) and Node(1) have the label `label1` and properties `p2` = true, `p3` = 'hi', `p4` = [6, 4]");
    }

    @Test
    void shouldMakeRelEntryConflicts() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2);
        IndexEntryConflictException e = new IndexEntryConflictException(schema, 0L, 1L, value);

        assertThat(e)
                .hasMessage(
                        "Both Relationship(0) and Relationship(1) have the type `RelationshipType[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("Both Relationship(0) and Relationship(1) have the type `type1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeEntryConflictsForOneRel() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2);
        IndexEntryConflictException e =
                new IndexEntryConflictException(schema, 0L, StatementConstants.NO_SUCH_RELATIONSHIP, value);

        assertThat(e)
                .hasMessage(
                        "Relationship(0) already exists with type `RelationshipType[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("Relationship(0) already exists with type `type1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeAnonymousEntryConflictsForNewRelUnknown() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2);
        IndexEntryConflictException e = new IndexEntryConflictException(
                schema, StatementConstants.NO_SUCH_RELATIONSHIP, StatementConstants.NO_SUCH_RELATIONSHIP, value);

        assertThat(e)
                .hasMessage(
                        "A Relationship already exists with type `RelationshipType[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo("A Relationship already exists with type `type1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeAnonymousEntryConflictsForNewRelKnown() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2);
        IndexEntryConflictException e =
                new IndexEntryConflictException(schema, StatementConstants.NO_SUCH_RELATIONSHIP, 0L, value);

        assertThat(e)
                .hasMessage(
                        "Both another Relationship and Relationship(0) have the type `RelationshipType[1]` and property `PropertyKey[2]` = 'hi'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo(
                        "Both another Relationship and Relationship(0) have the type `type1` and property `p2` = 'hi'");
    }

    @Test
    void shouldMakeCompositeRelEntryConflicts() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2, 3, 4);
        ValueTuple values = ValueTuple.of(true, "hi", new long[] {6L, 4L});
        IndexEntryConflictException e = new IndexEntryConflictException(schema, 0L, 1L, values);

        assertThat(e)
                .hasMessage(
                        "Both Relationship(0) and Relationship(1) have the type `RelationshipType[1]` and properties `PropertyKey[2]` = true, `PropertyKey[3]` = 'hi', `PropertyKey[4]` = [6, 4]");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo(
                        "Both Relationship(0) and Relationship(1) have the type `type1` and properties `p2` = true, `p3` = 'hi', `p4` = [6, 4]");
    }

    @Test
    void shouldNotThrowWhenMessageContainsAPercent() {
        SchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, 2, 3, 4);
        ValueTuple values = ValueTuple.of(true, "hi", "100%");
        IndexEntryConflictException e = new IndexEntryConflictException(schema, 0L, 1L, values);

        assertThat(e)
                .hasMessage(
                        "Both Relationship(0) and Relationship(1) have the type `RelationshipType[1]` and properties `PropertyKey[2]` = true, `PropertyKey[3]` = 'hi', `PropertyKey[4]` = '100%'");
        assertThat(e.getUserMessage(tokens))
                .isEqualTo(
                        "Both Relationship(0) and Relationship(1) have the type `type1` and properties `p2` = true, `p3` = 'hi', `p4` = '100%'");
    }
}
