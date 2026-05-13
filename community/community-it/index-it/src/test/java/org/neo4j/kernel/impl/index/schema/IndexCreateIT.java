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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SemanticSearchSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.schema.RepeatedLabelInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedRelationshipTypeInSchemaException;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

public class IndexCreateIT extends KernelIntegrationTest {
    private static final IndexCreator INDEX_CREATOR =
            (schemaWrite, schema, provider, name) -> schemaWrite.indexCreate(IndexPrototype.forSchema(schema)
                    .withIndexProvider(schemaWrite.indexProviderByName(provider))
                    .withName(name));
    private static final IndexCreator UNIQUE_CONSTRAINT_CREATOR =
            (schemaWrite, schema, provider, name) -> schemaWrite.uniquePropertyConstraintCreate(
                    IndexPrototype.uniqueForSchema(schema, schemaWrite.indexProviderByName(provider))
                            .withName(name));

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldCreateIndexWithSpecificExistingProviderName(EntityType entityType) throws KernelException {
        shouldCreateWithSpecificExistingProviderName(INDEX_CREATOR, entityType);
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldCreateUniquePropertyConstraintWithSpecificExistingProviderName(EntityType entityType)
            throws KernelException {
        shouldCreateWithSpecificExistingProviderName(UNIQUE_CONSTRAINT_CREATOR, entityType);
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldFailCreateIndexWithNonExistentProviderName(EntityType entityType) throws KernelException {
        shouldFailWithNonExistentProviderName(INDEX_CREATOR, entityType);
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldFailCreateUniquePropertyConstraintWithNonExistentProviderName(EntityType entityType)
            throws KernelException {
        shouldFailWithNonExistentProviderName(UNIQUE_CONSTRAINT_CREATOR, entityType);
    }

    @Test
    void shouldFailCreateIndexWithDuplicateLabels() throws KernelException {
        // given
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int labelId0 = tokenWrite.labelGetOrCreateForName("Label0");
        int labelId1 = tokenWrite.labelGetOrCreateForName("Label1");
        int labelId2 = tokenWrite.labelGetOrCreateForName("Label2");
        int labelId3 = tokenWrite.labelGetOrCreateForName("Label3");
        int propId = tokenWrite.propertyKeyGetOrCreateForName("property");
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // when
        SemanticSearchSchemaDescriptor descriptor = SchemaDescriptors.forSemanticSearch(
                org.neo4j.common.EntityType.NODE,
                new int[] {labelId0, labelId1, labelId2, labelId1, labelId3},
                new int[] {propId});
        // then
        RepeatedLabelInSchemaException e = assertThrows(
                RepeatedLabelInSchemaException.class,
                () -> schemaWrite.indexCreate(IndexPrototype.forSchema(descriptor)));
        assertThat(e.gqlStatus()).isEqualTo("22N76");
        assertThat(e.statusDescription())
                .isEqualTo(
                        "error: data exception - index contains duplicated tokens. The index specified by '(:Label0|Label1|Label2|Label1|Label3 {property})' includes a label, relationship type, or property key with name 'Label1' more than once.");
    }

    @Test
    void shouldFailCreateIndexWithDuplicateRelationshipTypes() throws KernelException {
        // given
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int relTypeId0 = tokenWrite.relationshipTypeGetOrCreateForName("RELATIONSHIP0");
        int relTypeId1 = tokenWrite.relationshipTypeGetOrCreateForName("RELATIONSHIP1");
        int relTypeId2 = tokenWrite.relationshipTypeGetOrCreateForName("RELATIONSHIP2");
        int relTypeId3 = tokenWrite.relationshipTypeGetOrCreateForName("RELATIONSHIP3");
        int propId = tokenWrite.propertyKeyGetOrCreateForName("property");
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // when
        SemanticSearchSchemaDescriptor descriptor = SchemaDescriptors.forSemanticSearch(
                org.neo4j.common.EntityType.RELATIONSHIP,
                new int[] {relTypeId0, relTypeId1, relTypeId2, relTypeId1, relTypeId3},
                new int[] {propId});
        // then
        RepeatedRelationshipTypeInSchemaException e = assertThrows(
                RepeatedRelationshipTypeInSchemaException.class,
                () -> schemaWrite.indexCreate(IndexPrototype.forSchema(descriptor)));
        assertThat(e.gqlStatus()).isEqualTo("22N76");
        assertThat(e.statusDescription())
                .isEqualTo(
                        "error: data exception - index contains duplicated tokens. The index specified by '()-[:RELATIONSHIP0|RELATIONSHIP1|RELATIONSHIP2|RELATIONSHIP1|RELATIONSHIP3 {property}]-()' includes a label, relationship type, or property key with name 'RELATIONSHIP1' more than once.");
    }

    @Test
    void shouldFailCreateIndexWithDuplicateProperties() throws KernelException {
        // given
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int labelId = tokenWrite.labelGetOrCreateForName("Label");
        int propId0 = tokenWrite.propertyKeyGetOrCreateForName("property0");
        int propId1 = tokenWrite.propertyKeyGetOrCreateForName("property1");
        int propId2 = tokenWrite.propertyKeyGetOrCreateForName("property2");
        int propId3 = tokenWrite.propertyKeyGetOrCreateForName("property3");
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // when
        SemanticSearchSchemaDescriptor descriptor =
                SchemaDescriptors.forSemanticSearch(org.neo4j.common.EntityType.NODE, new int[] {labelId}, new int[] {
                    propId0, propId1, propId2, propId1, propId3
                });
        // then
        RepeatedPropertyInSchemaException e = assertThrows(
                RepeatedPropertyInSchemaException.class,
                () -> schemaWrite.indexCreate(IndexPrototype.forSchema(descriptor)));
        assertThat(e.gqlStatus()).isEqualTo("22N76");
        assertThat(e.statusDescription())
                .isEqualTo(
                        "error: data exception - index contains duplicated tokens. The index specified by '(:Label {property0, property1, property2, property1, property3})' includes a label, relationship type, or property key with name 'property1' more than once.");
    }

    protected void shouldFailWithNonExistentProviderName(IndexCreator creator, EntityType entityType)
            throws KernelException {
        // given
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int entityTokenId = entityType.entityTokenGetOrCreate(tokenWrite);
        int propId = tokenWrite.propertyKeyGetOrCreateForName("property");
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // when
        assertThrows(
                IndexProviderNotFoundException.class,
                () -> creator.create(
                        schemaWrite,
                        entityType.createSchemaDescriptor(entityTokenId, propId),
                        "something-completely-different",
                        "index name"));
    }

    protected void shouldCreateWithSpecificExistingProviderName(IndexCreator creator, EntityType entityType)
            throws KernelException {
        // given
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int entityTokenId = entityType.entityTokenGetOrCreate(tokenWrite);
        int propId = tokenWrite.propertyKeyGetOrCreateForName("property");
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        SchemaDescriptor descriptor = entityType.createSchemaDescriptor(entityTokenId, propId);
        String provider = AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name();
        String indexName = "index-0";
        creator.create(schemaWrite, descriptor, provider, indexName);
        IndexDescriptor index = transaction.kernelTransaction().schemaRead().indexGetForName(indexName);

        // when
        commit();

        // then
        assertEquals(
                provider,
                indexingService
                        .getIndexProxy(index)
                        .getDescriptor()
                        .getIndexProvider()
                        .name());
    }

    protected interface IndexCreator {
        void create(SchemaWrite schemaWrite, SchemaDescriptor descriptor, String providerName, String indexName)
                throws KernelException;
    }

    interface EntityControl {
        int entityTokenGetOrCreate(TokenWrite tokenWrite) throws KernelException;

        SchemaDescriptor createSchemaDescriptor(int entityToken, int propertyKey);
    }

    protected enum EntityType implements EntityControl {
        NODE {
            @Override
            public int entityTokenGetOrCreate(TokenWrite tokenWrite) throws KernelException {
                return tokenWrite.labelGetOrCreateForName("Label0");
            }

            @Override
            public SchemaDescriptor createSchemaDescriptor(int entityToken, int propertyKey) {
                return forLabel(entityToken, propertyKey);
            }
        },
        RELATIONSHIP {
            @Override
            public int entityTokenGetOrCreate(TokenWrite tokenWrite) throws KernelException {
                return tokenWrite.relationshipTypeGetOrCreateForName("Type0");
            }

            @Override
            public SchemaDescriptor createSchemaDescriptor(int entityToken, int propertyKey) {
                return forRelType(entityToken, propertyKey);
            }
        }
    }
}
