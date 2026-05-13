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
import static org.neo4j.internal.schema.ConstraintType.UNIQUE;
import static org.neo4j.internal.schema.ConstraintType.UNIQUE_EXISTS;
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
 * Internal representation of a graph key or uniqueness constraint, including the schema unit it targets (eg. label-property combination)
 * and how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public class ConstraintDescriptorImplementation extends ConstraintDescriptorAdaptor
        implements ConstraintDescriptor, KeyConstraintDescriptor, UniquenessConstraintDescriptor {
    private final ConstraintType type;
    private final GraphTypeDependence graphTypeDependence;
    private final SchemaDescriptor schema;
    private final String name;
    private final Long ownedIndex;
    private final IndexType ownedIndexType;

    static KeyConstraintDescriptor makeUniqueExistsConstraint(SchemaDescriptor schema, IndexType indexType) {
        Preconditions.checkState(indexType != null, "Index type should be supplied for index-backed constraints");

        return new ConstraintDescriptorImplementation(
                UNIQUE_EXISTS, UNDESIGNATED, schema, NO_ID, null, null, indexType);
    }

    static UniquenessConstraintDescriptor makeUniqueConstraint(SchemaDescriptor schema, IndexType indexType) {
        Preconditions.checkState(indexType != null, "Index type should be supplied for index-backed constraints");

        return new ConstraintDescriptorImplementation(UNIQUE, UNDESIGNATED, schema, NO_ID, null, null, indexType);
    }

    private ConstraintDescriptorImplementation(
            ConstraintType type,
            GraphTypeDependence graphTypeDependence,
            SchemaDescriptor schema,
            long id,
            String name,
            Long ownedIndex,
            IndexType ownedIndexType) {
        super(id);
        this.type = type;
        this.graphTypeDependence = graphTypeDependence;
        this.schema = schema;
        this.name = name;
        this.ownedIndex = ownedIndex;
        this.ownedIndexType = ownedIndexType;
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
                tokenNameLookup, id, name, type, schema(), ownedIndex, null, null, null, mask);
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
    public final boolean equals(Object o) {
        if (!(o instanceof ConstraintDescriptor that)) {
            return false;
        }
        return equalsIgnoreName(that) && Objects.equals(this.name, that.getName());
    }

    @Override
    public final boolean equalsIgnoreName(ConstraintDescriptor that) {
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

        return !that.enforcesPropertyType();
    }

    // The constraints implementing this class must be unique on the combined fields constraintType, entityToken and
    // propertyToken(s)
    // It is also disallowed to have identical schema (entityToken and propertyToken(s)) and identical index type
    @Override
    public boolean conflictsWith(ConstraintDescriptor other) {
        if (!this.schema().equals(other.schema())) {
            return false;
        }

        if (this.isIndexBackedConstraint() && other.isIndexBackedConstraint()) {
            return this.asIndexBackedConstraint().indexType()
                    == other.asIndexBackedConstraint().indexType();
        }

        return this.type == other.type();
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, graphTypeDependence, schema, name);
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
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType);
    }

    @Override
    public ConstraintDescriptorImplementation withName(String name) {
        if (name == null) {
            return this;
        }
        name = SchemaNameUtil.sanitiseName(name);
        return new ConstraintDescriptorImplementation(
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType);
    }

    @Override
    public ConstraintDescriptorImplementation withOwnedIndexId(long ownedIndex) {
        Preconditions.checkState(
                ownedIndexType != null, "ConstraintDescriptor missing IndexType when connected to index");
        return new ConstraintDescriptorImplementation(
                type, graphTypeDependence, schema, id, name, ownedIndex, ownedIndexType);
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
