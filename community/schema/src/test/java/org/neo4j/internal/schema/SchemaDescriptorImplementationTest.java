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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.COMPLETE_ALL_TOKENS;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.ENTITY_TOKENS;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.PARTIAL_ANY_TOKEN;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.SINGLE_ENTITY_TOKEN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

final class SchemaDescriptorImplementationTest {

    @ParameterizedTest(name = "{index} {0} is {1}")
    @MethodSource("provideSchemaDescriptorTypeCases")
    void isSchemaDescriptorTypeTest(
            Supplier<SchemaDescriptor> descriptorSupplier,
            Class<? extends SchemaDescriptor> targetClazz,
            boolean expectedResult) {
        SchemaDescriptor schemaDescriptor = descriptorSupplier.get();
        assertEquals(expectedResult, schemaDescriptor.isSchemaDescriptorType(targetClazz));
    }

    @ParameterizedTest(name = "{index} {0} as {1}")
    @MethodSource("provideSchemaDescriptorTypeCases")
    void asSchemaDescriptorTypeTest(
            Supplier<SchemaDescriptor> descriptorSupplier,
            Class<? extends SchemaDescriptor> targetClazz,
            boolean expectedSuccess) {
        SchemaDescriptor schemaDescriptor = descriptorSupplier.get();
        if (expectedSuccess) {
            schemaDescriptor.asSchemaDescriptorType(targetClazz);
        } else {
            assertThrows(
                    IllegalStateException.class,
                    () -> schemaDescriptor.asSchemaDescriptorType(targetClazz),
                    "Cannot cast this schema");
        }
    }

    private static Stream<Arguments> provideSchemaDescriptorTypeCases() {
        final List<Class<? extends SchemaDescriptor>> targetClasses = Arrays.asList(
                SchemaDescriptor.class,
                LabelSchemaDescriptor.class,
                RelationTypeSchemaDescriptor.class,
                FulltextSchemaDescriptor.class,
                AnyTokenSchemaDescriptor.class,
                RelationshipEndpointSchemaDescriptor.class);

        final List<Supplier<SchemaDescriptor>> schemaSuppliers = Arrays.asList(
                LABEL_SCHEMA_DESCRIPTOR_SUPPLIER,
                RELATIONSHIP_SCHEMA_DESCRIPTOR_SUPPLIER,
                FULLTEXT_SCHEMA_DESCRIPTOR_SUPPLIER,
                ANY_TOKEN_SCHEMA_DESCRIPTOR_SUPPLIER,
                RELATIONSHIP_ENDPOINT_SCHEMA_DESCRIPTOR_SUPPLIER);

        final Map<String, Set<Class<? extends SchemaDescriptor>>> shouldBeTrue = new HashMap<>();
        shouldBeTrue.put(
                LABEL_SCHEMA_DESCRIPTOR_SUPPLIER.toString(),
                new HashSet<>(Arrays.asList(SchemaDescriptor.class, LabelSchemaDescriptor.class)));
        shouldBeTrue.put(
                RELATIONSHIP_SCHEMA_DESCRIPTOR_SUPPLIER.toString(),
                new HashSet<>(Arrays.asList(SchemaDescriptor.class, RelationTypeSchemaDescriptor.class)));
        shouldBeTrue.put(
                FULLTEXT_SCHEMA_DESCRIPTOR_SUPPLIER.toString(),
                new HashSet<>(Arrays.asList(SchemaDescriptor.class, FulltextSchemaDescriptor.class)));
        shouldBeTrue.put(
                ANY_TOKEN_SCHEMA_DESCRIPTOR_SUPPLIER.toString(),
                new HashSet<>(Arrays.asList(SchemaDescriptor.class, AnyTokenSchemaDescriptor.class)));
        shouldBeTrue.put(
                RELATIONSHIP_ENDPOINT_SCHEMA_DESCRIPTOR_SUPPLIER.toString(),
                new HashSet<>(Arrays.asList(SchemaDescriptor.class, RelationshipEndpointSchemaDescriptor.class)));

        final List<Arguments> cases = new ArrayList<>();
        for (var s : schemaSuppliers) {
            for (var clazz : targetClasses) {
                cases.add(Arguments.of(s, clazz, shouldBeTrue.get(s.toString()).contains(clazz)));
            }
        }
        return cases.stream();
    }

    private static final Supplier<SchemaDescriptor> LABEL_SCHEMA_DESCRIPTOR_SUPPLIER = supplierFor(
            new SchemaDescriptorImplementation(NODE, COMPLETE_ALL_TOKENS, new int[] {1}, new int[] {1}),
            "NODE_PROPERTY_SCHEME");

    private static final Supplier<SchemaDescriptor> RELATIONSHIP_SCHEMA_DESCRIPTOR_SUPPLIER = supplierFor(
            new SchemaDescriptorImplementation(RELATIONSHIP, COMPLETE_ALL_TOKENS, new int[] {1}, new int[] {1}),
            "RELATIONSHIP_PROPERTY_SCHEME");

    private static final Supplier<SchemaDescriptor> FULLTEXT_SCHEMA_DESCRIPTOR_SUPPLIER = supplierFor(
            new SchemaDescriptorImplementation(NODE, PARTIAL_ANY_TOKEN, new int[] {1}, new int[] {1}), "FULL_TEXT");

    private static final Supplier<SchemaDescriptor> ANY_TOKEN_SCHEMA_DESCRIPTOR_SUPPLIER = supplierFor(
            new SchemaDescriptorImplementation(NODE, ENTITY_TOKENS, new int[] {}, new int[] {}), "ANY_TOKEN");

    private static final Supplier<SchemaDescriptor> RELATIONSHIP_ENDPOINT_SCHEMA_DESCRIPTOR_SUPPLIER = supplierFor(
            new SchemaDescriptorImplementation(RELATIONSHIP, SINGLE_ENTITY_TOKEN, new int[] {1}, new int[] {}),
            "RELATIONSHIP_ENDPOINT");

    private static Supplier<SchemaDescriptor> supplierFor(SchemaDescriptor schemaDescriptor, String name) {
        return new Supplier<>() {
            @Override
            public SchemaDescriptor get() {
                return schemaDescriptor;
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
