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

import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_FULLTEXT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_TEXT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.POINT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaCommandUtils.backingIndex;
import static org.neo4j.internal.schema.SchemaCommandUtils.forSchema;
import static org.neo4j.internal.schema.SchemaCommandUtils.withName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.token.TokenHolders;

/**
 * Defines the different types of schema changes that can be performed in Cypher.
 */
public sealed interface SchemaCommand extends Serializable {

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

            IndexPrototype toPrototype(TokenHolders tokenHolders);

            // SchemaCommand.CreateRangeNodeIndex
            record NodeRange(String name, String label, List<String> properties, boolean ifNotExists)
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateRangeRelationshipIndex
            record RelationshipRange(String name, String type, List<String> properties, boolean ifNotExists)
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateTextNodeIndex
            record NodeText(String name, String label, String property, boolean ifNotExists) implements Create {
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateTextRelationshipIndex
            record RelationshipText(String name, String type, String property, boolean ifNotExists) implements Create {
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreatePointNodeIndex
            record NodePoint(String name, String label, String property, boolean ifNotExists, IndexConfig config)
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreatePointRelationshipIndex
            record RelationshipPoint(String name, String type, String property, boolean ifNotExists, IndexConfig config)
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateLookupIndex
            record NodeLookup(String name, boolean ifNotExists) implements Create {
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateLookupIndex
            record RelationshipLookup(String name, boolean ifNotExists) implements Create {
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
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateFulltextNodeIndex
            record NodeFulltext(
                    String name, List<String> labels, List<String> properties, boolean ifNotExists, IndexConfig config)
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
                                            SchemaDescriptors.forSemanticSearch(
                                                    EntityType.NODE,
                                                    tokenHolders.labelsForNames(labels),
                                                    tokenHolders.propertiesForName(properties)),
                                            DEFAULT_FULLTEXT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateFulltextRelationshipIndex
            record RelationshipFulltext(
                    String name, List<String> types, List<String> properties, boolean ifNotExists, IndexConfig config)
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
                                            SchemaDescriptors.forSemanticSearch(
                                                    EntityType.RELATIONSHIP,
                                                    tokenHolders.relationshipsForNames(types),
                                                    tokenHolders.propertiesForName(properties)),
                                            DEFAULT_FULLTEXT_DESCRIPTOR)
                                    .withIndexConfig(config),
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateVectorNodeIndex
            record NodeVector(
                    String name,
                    List<String> labels,
                    String property,
                    List<String> additionalProperties,
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists,
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
                    List<String> allProperties = new ArrayList<>(1 + additionalProperties.size());
                    allProperties.add(property);
                    allProperties.addAll(additionalProperties);
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forSemanticSearch(
                                                    EntityType.NODE,
                                                    tokenHolders.labelsForNames(labels),
                                                    tokenHolders.propertiesForName(allProperties)),
                                            providerDescriptor)
                                    .withIndexConfig(config),
                            tokenHolders);
                }
            }

            // SchemaCommand.CreateVectorRelationshipIndex
            record RelationshipVector(
                    String name,
                    List<String> types,
                    String property,
                    List<String> additionalProperties,
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists,
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
                    List<String> allProperties = new ArrayList<>(1 + additionalProperties.size());
                    allProperties.add(property);
                    allProperties.addAll(additionalProperties);
                    return withName(
                            name,
                            forSchema(
                                            this,
                                            SchemaDescriptors.forSemanticSearch(
                                                    EntityType.RELATIONSHIP,
                                                    tokenHolders.relationshipsForNames(types),
                                                    tokenHolders.propertiesForName(allProperties)),
                                            providerDescriptor)
                                    .withIndexConfig(config),
                            tokenHolders);
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
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists)
                    implements Create {

                public NodeUniqueness(String name, String label, List<String> properties, boolean ifNotExists) {
                    this(name, label, properties, RANGE_DESCRIPTOR, ifNotExists);
                }

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
                    LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                    IndexPrototype backingIndex =
                            backingIndex(schema, providerDescriptor == null ? RANGE_DESCRIPTOR : providerDescriptor);
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.uniqueForSchema(schema, backingIndex.getIndexType()),
                            tokenHolders);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record NodeExistence(String name, String label, String property, boolean isDependent, boolean ifNotExists)
                    implements Create {
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
                    LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                    ConstraintDescriptor constraintDescriptor = withName(
                            name, ConstraintDescriptorFactory.existsForSchema(schema, isDependent), tokenHolders);
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record NodeKey(
                    String name,
                    String label,
                    List<String> properties,
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists)
                    implements Create {

                public NodeKey(String name, String label, List<String> properties, boolean ifNotExists) {
                    this(name, label, properties, RANGE_DESCRIPTOR, ifNotExists);
                }

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
                    LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertiesForName(properties));
                    IndexPrototype backingIndex =
                            backingIndex(schema, providerDescriptor == null ? RANGE_DESCRIPTOR : providerDescriptor);
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.keyForSchema(schema, backingIndex.getIndexType()),
                            tokenHolders);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record NodePropertyType(
                    String name,
                    String label,
                    String property,
                    PropertyTypeSet propertyTypes,
                    boolean isDependent,
                    boolean ifNotExists)
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
                    LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(
                            tokenHolders.labelForName(label), tokenHolders.propertyForName(property));
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.typeForSchema(schema, propertyTypes, isDependent),
                            tokenHolders);
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record RelationshipUniqueness(
                    String name,
                    String type,
                    List<String> properties,
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists)
                    implements Create {

                public RelationshipUniqueness(String name, String type, List<String> properties, boolean ifNotExists) {
                    this(name, type, properties, RANGE_DESCRIPTOR, ifNotExists);
                }

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
                    RelationTypeSchemaDescriptor schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                    IndexPrototype backingIndex =
                            backingIndex(schema, providerDescriptor == null ? RANGE_DESCRIPTOR : providerDescriptor);
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.uniqueForSchema(schema, backingIndex.getIndexType()),
                            tokenHolders);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record RelationshipExistence(
                    String name, String type, String property, boolean isDependent, boolean ifNotExists)
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
                    RelationTypeSchemaDescriptor schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                    ConstraintDescriptor constraintDescriptor = withName(
                            name, ConstraintDescriptorFactory.existsForSchema(schema, isDependent), tokenHolders);
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record RelationshipKey(
                    String name,
                    String type,
                    List<String> properties,
                    IndexProviderDescriptor providerDescriptor,
                    boolean ifNotExists)
                    implements Create {

                public RelationshipKey(String name, String type, List<String> properties, boolean ifNotExists) {
                    this(name, type, properties, RANGE_DESCRIPTOR, ifNotExists);
                }

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
                    RelationTypeSchemaDescriptor schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertiesForName(properties));
                    IndexPrototype backingIndex =
                            backingIndex(schema, providerDescriptor == null ? RANGE_DESCRIPTOR : providerDescriptor);
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.keyForSchema(schema, backingIndex.getIndexType()),
                            tokenHolders);
                    return new ConstraintPrototype(
                            constraintDescriptor, backingIndex.withName(constraintDescriptor.getName()));
                }
            }

            record RelationshipPropertyType(
                    String name,
                    String type,
                    String property,
                    PropertyTypeSet propertyTypes,
                    boolean isDependent,
                    boolean ifNotExists)
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
                    RelationTypeSchemaDescriptor schema = SchemaDescriptors.forRelType(
                            tokenHolders.relationshipForName(type), tokenHolders.propertyForName(property));
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.typeForSchema(schema, propertyTypes, isDependent),
                            tokenHolders);
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }

            record NodeLabelExistence(String name, String label, String requiredLabel, boolean ifNotExists)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.NODE;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.NODE_LABEL_EXISTENCE;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    int labelId = tokenHolders.labelForName(label);
                    int requiredLabelId = tokenHolders.labelForName(requiredLabel);
                    NodeLabelExistenceSchemaDescriptor schema = SchemaDescriptors.forNodeLabelExistence(labelId);
                    return new ConstraintPrototype(withName(
                            name,
                            ConstraintDescriptorFactory.nodeLabelExistenceForSchema(schema, requiredLabelId),
                            tokenHolders));
                }
            }

            record RelationshipEndpointLabel(
                    String name, String type, String requiredLabel, EndpointType endpointType, boolean ifNotExists)
                    implements Create {
                @Override
                public EntityType entityType() {
                    return EntityType.RELATIONSHIP;
                }

                @Override
                public ConstraintType constraintType() {
                    return ConstraintType.RELATIONSHIP_ENDPOINT_LABEL;
                }

                @Override
                public boolean hasBackingIndex() {
                    return false;
                }

                @Override
                public ConstraintPrototype toPrototype(TokenHolders tokenHolders) {
                    int relationshipId = tokenHolders.relationshipForName(type);
                    int requiredLabelId = tokenHolders.labelForName(requiredLabel);
                    RelationshipEndpointLabelSchemaDescriptor schema =
                            SchemaDescriptors.forRelationshipEndpointLabel(relationshipId);
                    ConstraintDescriptor constraintDescriptor = withName(
                            name,
                            ConstraintDescriptorFactory.relationshipEndpointLabelForSchema(
                                    schema, requiredLabelId, endpointType),
                            tokenHolders);
                    return new ConstraintPrototype(constraintDescriptor);
                }
            }
        }
    }

    record GraphType(
            Set<? extends ConstraintCommand.Create> addedConstraints,
            Set<ConstraintCommand.Drop> droppedConstraints,
            Operation op)
            implements SchemaCommand.ConstraintCommand {
        @Override
        public String name() {
            return op.name() + " "
                    + String.join(
                            ",",
                            addedConstraints.stream().map(SchemaCommand::name).toList()) + ";"
                    + String.join(
                            ",",
                            droppedConstraints.stream().map(SchemaCommand::name).toList());
        }

        @Override
        public String toString() {
            return "GraphType[addedConstraints="
                    + String.join(
                            ",\n",
                            addedConstraints.stream().map(Object::toString).toList()) + ",\n droppedConstraints="
                    + String.join(
                            ",\n",
                            droppedConstraints.stream().map(Object::toString).toList()) + ", op=" + op + "]";
        }

        public enum Operation {
            ADD,
            DROP,
            SET,
            ALTER
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
