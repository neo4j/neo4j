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
package org.neo4j.internal.schema.constraints;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.ConstraintType.EXISTS;
import static org.neo4j.internal.schema.ConstraintType.PROPERTY_TYPE;
import static org.neo4j.internal.schema.ConstraintType.UNIQUE;
import static org.neo4j.internal.schema.ConstraintType.UNIQUE_EXISTS;
import static org.neo4j.internal.schema.GraphTypeDependence.DEPENDENT;
import static org.neo4j.internal.schema.GraphTypeDependence.INDEPENDENT;
import static org.neo4j.internal.schema.GraphTypeDependence.UNDESIGNATED;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.Objects;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.string.Mask;
import org.neo4j.util.Preconditions;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public class ConstraintDescriptorImplementation
        implements ConstraintDescriptor,
                ExistenceConstraintDescriptor,
                KeyConstraintDescriptor,
                UniquenessConstraintDescriptor,
                TypeConstraintDescriptor {
    private final ConstraintType type;
    private final GraphTypeDependence graphTypeDependence;
    private final SchemaDescriptor schema;
    private final long id;
    private final String name;
    private final Long ownedIndex;
    private final IndexType ownedIndexType;
    private final PropertyTypeSet propertyType;

    static ConstraintDescriptorImplementation makeExistsConstraint(SchemaDescriptor schema) {
        return makeExistsConstraint(schema, false);
    }

    static ConstraintDescriptorImplementation makeExistsConstraint(SchemaDescriptor schema, boolean isDependent) {
        return new ConstraintDescriptorImplementation(
                EXISTS, isDependent ? DEPENDENT : INDEPENDENT, schema, NO_ID, null, null, null, null);
    }

    static KeyConstraintDescriptor makeUniqueExistsConstraint(SchemaDescriptor schema, IndexType indexType) {
        Preconditions.checkState(indexType != null, "Index type should be supplied for index-backed constraints");

        return new ConstraintDescriptorImplementation(
                UNIQUE_EXISTS, UNDESIGNATED, schema, NO_ID, null, null, indexType, null);
    }

    static UniquenessConstraintDescriptor makeUniqueConstraint(SchemaDescriptor schema, IndexType indexType) {
        Preconditions.checkState(indexType != null, "Index type should be supplied for index-backed constraints");

        return new ConstraintDescriptorImplementation(UNIQUE, UNDESIGNATED, schema, NO_ID, null, null, indexType, null);
    }

    static TypeConstraintDescriptor makePropertyTypeConstraint(SchemaDescriptor schema, PropertyTypeSet propertyType) {
        return makePropertyTypeConstraint(schema, propertyType, false);
    }

    static TypeConstraintDescriptor makePropertyTypeConstraint(
            SchemaDescriptor schema, PropertyTypeSet propertyType, boolean isDependent) {
        Preconditions.checkState(
                propertyType != null, "Property types should be supplied for property type constraints");
        return new ConstraintDescriptorImplementation(
                PROPERTY_TYPE, isDependent ? DEPENDENT : INDEPENDENT, schema, NO_ID, null, null, null, propertyType);
    }

    private ConstraintDescriptorImplementation(
            ConstraintType type,
            GraphTypeDependence graphTypeDependence,
            SchemaDescriptor schema,
            long id,
            String name,
            Long ownedIndex,
            IndexType ownedIndexType,
            PropertyTypeSet propertyType) {
        this.type = type;
        this.graphTypeDependence = graphTypeDependence;
        this.schema = schema;
        this.id = id;
        this.name = name;
        this.ownedIndex = ownedIndex;
        this.ownedIndexType = ownedIndexType;
        this.propertyType = propertyType;
    }

    // METHODS

    @Override
    public ConstraintType type() {
        return type;
    }

    @Override
    public GraphTypeDependence graphTypeDependence() {
        return graphTypeDependence;
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public boolean enforcesUniqueness() {
        return type.enforcesUniqueness();
    }

    @Override
    public boolean enforcesPropertyExistence() {
        return type.enforcesPropertyExistence();
    }

    @Override
    public boolean enforcesPropertyType() {
        return type.enforcesPropertyType();
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user-friendly description of this constraint.
     */
    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return userDescription(tokenNameLookup, Mask.NO);
    }

    private String userDescription(TokenNameLookup tokenNameLookup, Mask mask) {
        return SchemaUserDescription.forConstraint(
                tokenNameLookup, id, name, type, schema(), ownedIndex, propertyType, mask);
    }

    @Override
    public boolean isNodePropertyTypeConstraint() {
        return schema.entityType() == NODE && type == PROPERTY_TYPE;
    }

    @Override
    public boolean isRelationshipPropertyTypeConstraint() {
        return schema.entityType() == RELATIONSHIP && type == PROPERTY_TYPE;
    }

    @Override
    public boolean isPropertyTypeConstraint() {
        return type == PROPERTY_TYPE;
    }

    @Override
    public TypeConstraintDescriptor asPropertyTypeConstraint() {
        if (!isPropertyTypeConstraint()) {
            throw conversionException(TypeConstraintDescriptor.class);
        }
        return this;
    }

    @Override
    public boolean isPropertyExistenceConstraint() {
        return type == EXISTS;
    }

    @Override
    public boolean isRelationshipPropertyExistenceConstraint() {
        return schema.entityType() == RELATIONSHIP && type == EXISTS;
    }

    @Override
    public boolean isNodePropertyExistenceConstraint() {
        return schema.entityType() == NODE && type == EXISTS;
    }

    @Override
    public ExistenceConstraintDescriptor asPropertyExistenceConstraint() {
        if (!isPropertyExistenceConstraint()) {
            throw conversionException(ExistenceConstraintDescriptor.class);
        }
        return this;
    }

    @Override
    public boolean isUniquenessConstraint() {
        return type == UNIQUE;
    }

    @Override
    public boolean isNodeUniquenessConstraint() {
        return schema.entityType() == NODE && type == UNIQUE;
    }

    @Override
    public boolean isRelationshipUniquenessConstraint() {
        return schema.entityType() == RELATIONSHIP && type == UNIQUE;
    }

    @Override
    public UniquenessConstraintDescriptor asUniquenessConstraint() {
        if (!isUniquenessConstraint()) {
            throw conversionException(UniquenessConstraintDescriptor.class);
        }
        return this;
    }

    @Override
    public boolean isNodeKeyConstraint() {
        return schema.entityType() == NODE && type == ConstraintType.UNIQUE_EXISTS;
    }

    @Override
    public boolean isRelationshipKeyConstraint() {
        return schema.entityType() == RELATIONSHIP && type == ConstraintType.UNIQUE_EXISTS;
    }

    @Override
    public boolean isIndexBackedConstraint() {
        return type.enforcesUniqueness();
    }

    @Override
    public IndexBackedConstraintDescriptor asIndexBackedConstraint() {
        if (!isIndexBackedConstraint()) {
            throw conversionException(IndexBackedConstraintDescriptor.class);
        }
        return this;
    }

    @Override
    public boolean isKeyConstraint() {
        return isNodeKeyConstraint() || isRelationshipKeyConstraint();
    }

    @Override
    public KeyConstraintDescriptor asKeyConstraint() {
        if (!isKeyConstraint()) {
            throw conversionException(KeyConstraintDescriptor.class);
        }
        return this;
    }

    @Override
    public boolean isRelationshipEndpointConstraint() {
        return true;
    }

    @Override
    public RelationshipEndpointConstraintDescriptor asRelationshipEndpointConstraint() {
        throw conversionException(RelationshipEndpointConstraintDescriptor.class);
    }

    private IllegalStateException conversionException(Class<? extends ConstraintDescriptor> targetType) {
        return new IllegalStateException("Cannot cast this schema to a " + targetType
                + " because it does not match that structure: " + this + ".");
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof ConstraintDescriptor that)) {
            return false;
        }

        if (this.type != that.type()) {
            return false;
        }

        if (this.graphTypeDependence != that.graphTypeDependence()) {
            return false;
        }

        if (!this.schema().equals(that.schema())) {
            return false;
        }

        if (that.isIndexBackedConstraint()
                && !this.indexType().equals(that.asIndexBackedConstraint().indexType())) {
            return false;
        }

        if (that.enforcesPropertyType()
                && !this.propertyType.equals(that.asPropertyTypeConstraint().propertyType())) {
            return false;
        }

        return true;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, graphTypeDependence, schema);
    }

    @Override
    public long getId() {
        if (id == NO_ID) {
            throw new IllegalStateException("This constraint descriptor have no id assigned: " + this);
        }
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean hasOwnedIndexId() {
        return ownedIndex != null;
    }

    @Override
    public long ownedIndexId() {
        if (ownedIndex == null) {
            throw new IllegalStateException("This constraint does not own an index.");
        }
        return ownedIndex;
    }

    @Override
    public IndexType indexType() {
        if (ownedIndexType == null) {
            throw new IllegalStateException("This constraint does not own an index.");
        }
        return ownedIndexType;
    }

    @Override
    public ConstraintDescriptorImplementation withId(long id) {
        return new ConstraintDescriptorImplementation(
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType, propertyType);
    }

    @Override
    public ConstraintDescriptorImplementation withName(String name) {
        if (name == null) {
            return this;
        }
        name = SchemaNameUtil.sanitiseName(name);
        return new ConstraintDescriptorImplementation(
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType, propertyType);
    }

    @Override
    public PropertyTypeSet propertyType() {
        if (!enforcesPropertyType()) {
            throw new IllegalStateException("This constraint does not enforce property types.");
        }
        return propertyType;
    }

    @Override
    public ConstraintDescriptorImplementation withOwnedIndexId(long ownedIndex) {
        Preconditions.checkState(
                ownedIndexType != null, "ConstraintDescriptor missing IndexType when connected to index");
        return new ConstraintDescriptorImplementation(
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType, propertyType);
    }

    @Override
    public String toString() {
        return toString(Mask.NO);
    }

    @Override
    public String toString(Mask mask) {
        // TOKEN_ID_NAME_LOOKUP makes sure we don't include schema token names, regardless of masking
        return userDescription(TOKEN_ID_NAME_LOOKUP, mask);
    }
}
