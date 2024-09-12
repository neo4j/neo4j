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

import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_TEXT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_VECTOR_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.POINT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaCommandUtils.backingIndex;
import static org.neo4j.internal.schema.SchemaCommandUtils.forSchema;
import static org.neo4j.internal.schema.SchemaCommandUtils.withName;

import java.util.List;
import java.util.Optional;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.token.TokenHolders;

/**
 * Defines the different types of schema changes that can be performed in Cypher.
 */
public sealed interface SchemaCommand {

    /**
     * @return the name of the schema change
     */
    String name();

    sealed interface IndexCommand extends SchemaCommand {
        // SchemaCommand.DropIndexOnName
        record Drop(String name, boolean ifExists) implements IndexCommand {}

        sealed interface Create extends IndexCommand {
            EntityType entityType();

            IndexType indexType();

            boolean ifNotExists();

            Optional<IndexProviderDescriptor> provider();

            IndexPrototype toPrototype(TokenHolders tokenHolders);

            // SchemaCommand.CreateRangeNodeIndex
            record NodeRange(
                    String name,
                    String label,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.RANGE;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                    this,
                                    SchemaDescriptors.forLabel(
                                            tokenHolders.labelForName(label),
                                            tokenHolders.propertiesForName(properties)),
                                    RANGE_DESCRIPTOR),
                            List.of(label),
                            properties);
                }
            }

            // SchemaCommand.CreateRangeRelationshipIndex
            record RelationshipRange(
                    String name,
                    String type,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.RANGE;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                    this,
                                    SchemaDescriptors.forRelType(
                                            tokenHolders.relationshipForName(type),
                                            tokenHolders.propertiesForName(properties)),
                                    RANGE_DESCRIPTOR),
                            List.of(type),
                            properties);
                }
            }

            // SchemaCommand.CreateTextNodeIndex
            record NodeText(
                    String name,
                    String label,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.TEXT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                    this,
                                    SchemaDescriptors.forLabel(
                                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property)),
                                    DEFAULT_TEXT_DESCRIPTOR),
                            List.of(label),
                            List.of(property));
                }
            }

            // SchemaCommand.CreateTextRelationshipIndex
            record RelationshipText(
                    String name,
                    String type,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.TEXT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                    this,
                                    SchemaDescriptors.forRelType(
                                            tokenHolders.relationshipForName(type),
                                            tokenHolders.propertyForName(property)),
                                    DEFAULT_TEXT_DESCRIPTOR),
                            List.of(type),
                            List.of(property));
                }
            }

            // SchemaCommand.CreatePointNodeIndex
            record NodePoint(
                    String name,
                    String label,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.POINT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forLabel(
                                                    tokenHolders.labelForName(label),
                                                    tokenHolders.propertyForName(property)),
                                            POINT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            List.of(label),
                            List.of(property));
                }
            }

            // SchemaCommand.CreatePointRelationshipIndex
            record RelationshipPoint(
                    String name,
                    String type,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.POINT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forRelType(
                                                    tokenHolders.relationshipForName(type),
                                                    tokenHolders.propertyForName(property)),
                                            POINT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            List.of(type),
                            List.of(property));
                }
            }

            // SchemaCommand.CreateLookupIndex
            record NodeLookup(String name, boolean ifNotExists, Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.LOOKUP;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(this, SchemaDescriptors.forAnyEntityTokens(EntityType.NODE), TOKEN_DESCRIPTOR),
                            List.of(),
                            List.of());
                }
            }

            // SchemaCommand.CreateLookupIndex
            record RelationshipLookup(String name, boolean ifNotExists, Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.LOOKUP;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                    this,
                                    SchemaDescriptors.forAnyEntityTokens(EntityType.RELATIONSHIP),
                                    TOKEN_DESCRIPTOR),
                            List.of(),
                            List.of());
                }
            }

            // SchemaCommand.CreateFulltextNodeIndex
            record NodeFulltext(
                    String name,
                    List<String> labels,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.FULLTEXT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.fulltext(
                                                    EntityType.NODE,
                                                    tokenHolders.labelsForNames(labels),
                                                    tokenHolders.propertiesForName(properties)),
                                            FULLTEXT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            labels,
                            properties);
                }
            }

            // SchemaCommand.CreateFulltextRelationshipIndex
            record RelationshipFulltext(
                    String name,
                    List<String> types,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.FULLTEXT;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.fulltext(
                                                    EntityType.RELATIONSHIP,
                                                    tokenHolders.relationshipsForNames(types),
                                                    tokenHolders.propertiesForName(properties)),
                                            FULLTEXT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            types,
                            properties);
                }
            }

            // SchemaCommand.CreateVectorNodeIndex
            record NodeVector(
                    String name,
                    String label,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.VECTOR;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forLabel(
                                                    tokenHolders.labelForName(label),
                                                    tokenHolders.propertyForName(property)),
                                            DEFAULT_VECTOR_DESCRIPTOR)
                                    .withIndexConfig(config),
                            List.of(label),
                            List.of(property));
                }
            }

            // SchemaCommand.CreateVectorRelationshipIndex
            record RelationshipVector(
                    String name,
                    String type,
                    String property,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider,
                    IndexConfig config)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public IndexType indexType() {
                    return IndexType.VECTOR;
                }

                @Override
                public IndexPrototype toPrototype(TokenHolders tokenHolders) {
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forRelType(
                                                    tokenHolders.relationshipForName(type),
                                                    tokenHolders.propertyForName(property)),
                                            DEFAULT_VECTOR_DESCRIPTOR)
                                    .withIndexConfig(config),
                            List.of(type),
                            List.of(property));
                }
            }
        }
    }

    sealed interface ConstraintCommand extends SchemaCommand {
        // SchemaCommand.DropConstraintOnName
        record Drop(String name, boolean ifExists) implements ConstraintCommand {}

        sealed interface Create extends ConstraintCommand {

            record ConstraintPrototype(ConstraintDescriptor descriptor, IndexPrototype backingIndex) {
                public ConstraintPrototype(ConstraintDescriptor descriptor) {
                    this(descriptor, null);
                }
            }

            EntityType entityType();

            ConstraintType constraintType();

            boolean ifNotExists();

            boolean hasBackingIndex();

            ConstraintPrototype toPrototype(TokenHolders tokenHolders);

            record NodeUniqueness(
                    String name,
                    String label,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.UNIQUE;
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                    final var backingIndex = backingIndex(schema, provider);
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.uniqueForSchema(schema, backingIndex.getIndexType()),
                            List.of(label),
                            properties);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record NodeExistence(String name, String label, String property, boolean ifNotExists) implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.EXISTS;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.existsForSchema(schema, false),
                            List.of(label),
                            List.of(property));
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record NodeKey(
                    String name,
                    String label,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.UNIQUE_EXISTS;
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                    final var backingIndex = backingIndex(schema, provider);
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.keyForSchema(schema, backingIndex.getIndexType()),
                            List.of(label),
                            properties);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record NodePropertyType(
                    String name, String label, String property, PropertyTypeSet propertyTypes, boolean ifNotExists)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.PROPERTY_TYPE;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.typeForSchema(schema, propertyTypes, false),
                            List.of(label),
                            List.of(property));
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record RelationshipUniqueness(
                    String name,
                    String type,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.UNIQUE;
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                    final var backingIndex = backingIndex(schema, provider);
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.uniqueForSchema(schema, backingIndex.getIndexType()),
                            List.of(type),
                            properties);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record RelationshipExistence(String name, String type, String property, boolean ifNotExists)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.EXISTS;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.existsForSchema(schema, false),
                            List.of(type),
                            List.of(property));
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record RelationshipKey(
                    String name,
                    String type,
                    List<String> properties,
                    boolean ifNotExists,
                    Optional<IndexProviderDescriptor> provider)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.UNIQUE_EXISTS;
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                    final var backingIndex = backingIndex(schema, provider);
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.keyForSchema(schema, backingIndex.getIndexType()),
                            List.of(type),
                            properties);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record RelationshipPropertyType(
                    String name, String type, String property, PropertyTypeSet propertyTypes, boolean ifNotExists)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.PROPERTY_TYPE;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    final var schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                    final var constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.typeForSchema(schema, propertyTypes, false),
                            List.of(type),
                            List.of(property));
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }
        }
    }

    class SchemaCommandReaderException extends RuntimeException {
        public SchemaCommandReaderException(String message) {
            super(message);
        }

        public SchemaCommandReaderException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
