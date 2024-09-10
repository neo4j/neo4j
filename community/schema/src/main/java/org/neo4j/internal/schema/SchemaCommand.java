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

import java.util.List;
import java.util.Optional;
import org.neo4j.common.EntityType;
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

            SchemaDescriptor toSchema(TokenHolders tokenHolders);

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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forAnyEntityTokens(EntityType.NODE);
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forAnyEntityTokens(EntityType.RELATIONSHIP);
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.fulltext(
                            EntityType.NODE,
                            tokenHolders.labelsForNames(labels),
                            tokenHolders.propertiesForName(properties));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.fulltext(
                            EntityType.RELATIONSHIP,
                            tokenHolders.relationshipsForNames(types),
                            tokenHolders.propertiesForName(properties));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                }
            }
        }
    }

    sealed interface ConstraintCommand extends SchemaCommand {
        // SchemaCommand.DropConstraintOnName
        record Drop(String name, boolean ifExists) implements ConstraintCommand {}

        sealed interface Create extends ConstraintCommand {
            EntityType entityType();

            ConstraintType constraintType();

            boolean ifNotExists();

            SchemaDescriptor toSchema(TokenHolders tokenHolders);

            boolean hasBackingIndex();

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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                }

                @Override
                public boolean hasBackingIndex() {
                    return true;
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
                public SchemaDescriptor toSchema(TokenHolders tokenHolders) {
                    return SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
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
