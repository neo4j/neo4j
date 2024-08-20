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

import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.LabelCoexistenceSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.string.Mask;

final class LabelCoexistenceConstraintDescriptorImplementation implements LabelCoexistenceConstraintDescriptor {
    private final long id;
    private final String name;
    private final LabelCoexistenceSchemaDescriptor schema;
    private final int requiredLabelId;

    LabelCoexistenceConstraintDescriptorImplementation(
            LabelCoexistenceSchemaDescriptor schema, long id, int requiredLabelId, String name) {
        if (requiredLabelId < 0) {
            throw new IllegalArgumentException("requiredLabelId cannot be negative");
        }
        this.schema = requireNonNull(schema, "LabelCoexistenceSchemaDescriptor cannot be null");
        if (requiredLabelId == schema.getLabelId()) {
            throw new IllegalArgumentException("requiredLabelId cannot be same as schema labelId");
        }
        this.requiredLabelId = requiredLabelId;
        this.id = id;
        this.name = name;
    }

    static LabelCoexistenceConstraintDescriptor make(LabelCoexistenceSchemaDescriptor schema, int requiredLabelId) {
        return new LabelCoexistenceConstraintDescriptorImplementation(schema, NO_ID, requiredLabelId, null);
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public ConstraintType type() {
        return ConstraintType.LABEL_COEXISTENCE;
    }

    @Override
    public GraphTypeDependence graphTypeDependence() {
        return GraphTypeDependence.DEPENDENT;
    }

    @Override
    public boolean enforcesUniqueness() {
        return false;
    }

    @Override
    public boolean enforcesPropertyExistence() {
        return false;
    }

    @Override
    public boolean enforcesPropertyType() {
        return false;
    }

    @Override
    public boolean isPropertyTypeConstraint() {
        return false;
    }

    @Override
    public boolean isRelationshipEndpointConstraint() {
        return false;
    }

    @Override
    public boolean isLabelCoexistenceConstraint() {
        return true;
    }

    @Override
    public boolean isNodePropertyTypeConstraint() {
        return false;
    }

    @Override
    public boolean isRelationshipPropertyTypeConstraint() {
        return false;
    }

    @Override
    public TypeConstraintDescriptor asPropertyTypeConstraint() {
        throw conversionException(TypeConstraintDescriptor.class);
    }

    @Override
    public boolean isPropertyExistenceConstraint() {
        return false;
    }

    @Override
    public boolean isRelationshipPropertyExistenceConstraint() {
        return false;
    }

    @Override
    public boolean isNodePropertyExistenceConstraint() {
        return false;
    }

    @Override
    public ExistenceConstraintDescriptor asPropertyExistenceConstraint() {
        throw conversionException(ExistenceConstraintDescriptor.class);
    }

    @Override
    public boolean isUniquenessConstraint() {
        return false;
    }

    @Override
    public boolean isNodeUniquenessConstraint() {
        return false;
    }

    @Override
    public boolean isRelationshipUniquenessConstraint() {
        return false;
    }

    @Override
    public UniquenessConstraintDescriptor asUniquenessConstraint() {
        throw conversionException(UniquenessConstraintDescriptor.class);
    }

    @Override
    public boolean isNodeKeyConstraint() {
        return false;
    }

    @Override
    public boolean isRelationshipKeyConstraint() {
        return false;
    }

    @Override
    public boolean isIndexBackedConstraint() {
        return false;
    }

    @Override
    public IndexBackedConstraintDescriptor asIndexBackedConstraint() {
        throw conversionException(IndexBackedConstraintDescriptor.class);
    }

    @Override
    public boolean isKeyConstraint() {
        return false;
    }

    @Override
    public KeyConstraintDescriptor asKeyConstraint() {
        throw conversionException(KeyConstraintDescriptor.class);
    }

    @Override
    public ConstraintDescriptor withId(long newId) {
        return new LabelCoexistenceConstraintDescriptorImplementation(schema, newId, requiredLabelId, name);
    }

    @Override
    public ConstraintDescriptor withName(String newName) {
        if (newName == null) {
            return this;
        }
        newName = SchemaNameUtil.sanitiseName(newName);
        return new LabelCoexistenceConstraintDescriptorImplementation(schema, id, requiredLabelId, newName);
    }

    @Override
    public IndexBackedConstraintDescriptor withOwnedIndexId(long id) {
        throw new NotImplementedException();
    }

    @Override
    public RelationshipEndpointConstraintDescriptor asRelationshipEndpointConstraint() {
        throw conversionException(RelationshipEndpointConstraintDescriptor.class);
    }

    @Override
    public LabelCoexistenceConstraintDescriptor asLabelCoexistenceConstraint() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LabelCoexistenceConstraintDescriptor that)) {
            return false;
        }

        if (this.requiredLabelId != that.requiredLabelId()) {
            return false;
        }

        if (!this.schema().equals(that.schema())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
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
    public int requiredLabelId() {
        return requiredLabelId;
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

    private String userDescription(TokenNameLookup tokenNameLookup, Mask mask) {
        return SchemaUserDescription.forConstraint(
                tokenNameLookup, id, name, ConstraintType.LABEL_COEXISTENCE, schema, null, null, mask);
    }

    private IllegalStateException conversionException(Class<? extends ConstraintDescriptor> targetType) {
        return new IllegalStateException("Cannot cast this schema to a " + targetType
                + " because it does not match that structure: " + this + ".");
    }
}
