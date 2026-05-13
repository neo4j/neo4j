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
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.RelationshipEndpointLabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.string.Mask;

final class RelationshipEndpointLabelConstraintDescriptorImplementation extends ConstraintDescriptorAdaptor
        implements RelationshipEndpointLabelConstraintDescriptor {
    private final int endpointLabelId;
    private final String name;
    private final RelationshipEndpointLabelSchemaDescriptor schema;
    private final EndpointType endpointType;

    private RelationshipEndpointLabelConstraintDescriptorImplementation(
            RelationshipEndpointLabelSchemaDescriptor schema,
            long id,
            int endpointLabelId,
            String name,
            EndpointType endpointType) {
        super(id);
        if (endpointLabelId < 0) {
            throw new IllegalArgumentException("endpointLabelId cannot be negative");
        }
        this.schema = requireNonNull(schema, "SchemaDescriptor cannot be null");
        this.endpointType = requireNonNull(endpointType, "EndpointType cannot be null");
        this.endpointLabelId = endpointLabelId;
        this.name = name;
    }

    static RelationshipEndpointLabelConstraintDescriptor make(
            RelationshipEndpointLabelSchemaDescriptor schema, int endpointLabelId, EndpointType endpointType) {
        return new RelationshipEndpointLabelConstraintDescriptorImplementation(
                schema, NO_ID, endpointLabelId, null, endpointType);
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public ConstraintType type() {
        return ConstraintType.RELATIONSHIP_ENDPOINT_LABEL;
    }

    @Override
    public GraphTypeDependence graphTypeDependence() {
        return GraphTypeDependence.DEPENDENT;
    }

    @Override
    public boolean isRelationshipEndpointLabelConstraint() {
        return true;
    }

    @Override
    public RelationshipEndpointLabelConstraintDescriptor withId(long newId) {
        return new RelationshipEndpointLabelConstraintDescriptorImplementation(
                schema, newId, endpointLabelId, name, endpointType);
    }

    @Override
    public RelationshipEndpointLabelConstraintDescriptor withName(String newName) {
        if (newName == null) {
            return this;
        }
        newName = SchemaNameUtil.sanitiseName(newName);
        return new RelationshipEndpointLabelConstraintDescriptorImplementation(
                schema, id, endpointLabelId, newName, endpointType);
    }

    @Override
    public IndexBackedConstraintDescriptor withOwnedIndexId(long id) {
        throw new NotImplementedException();
    }

    @Override
    public RelationshipEndpointLabelConstraintDescriptor asRelationshipEndpointLabelConstraint() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RelationshipEndpointLabelConstraintDescriptor that)) {
            return false;
        }
        return equalsIgnoreName(that) && Objects.equals(this.name, that.getName());
    }

    @Override
    public boolean equalsIgnoreName(ConstraintDescriptor other) {
        // ugly, needed since equalsIgnoreName might be called from something else than equals
        if (!other.isRelationshipEndpointLabelConstraint()) {
            return false;
        }
        RelationshipEndpointLabelConstraintDescriptor that = other.asRelationshipEndpointLabelConstraint();
        if (this.endpointType != that.endpointType()) {
            return false;
        }

        if (this.endpointLabelId != that.endpointLabelId()) {
            return false;
        }

        return this.schema().equals(that.schema());
    }

    // For RelationshipEndpointConstraints we are allowed to have at most one constraint per EndpointType and RelType
    // There is no limitation on the other metadata fields.
    // Therefore, we only check for conflicts on EndpointType.
    @Override
    public boolean conflictsWith(ConstraintDescriptor other) {
        if (other.isRelationshipEndpointLabelConstraint()) {
            RelationshipEndpointLabelConstraintDescriptor that = other.asRelationshipEndpointLabelConstraint();
            return this.endpointType == that.endpointType() && this.schema().equals(that.schema());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, endpointType, endpointLabelId, name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int endpointLabelId() {
        return endpointLabelId;
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

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return userDescription(tokenNameLookup, Mask.NO);
    }

    private String userDescription(TokenNameLookup tokenNameLookup, Mask mask) {
        return SchemaUserDescription.forConstraint(
                tokenNameLookup,
                id,
                name,
                ConstraintType.RELATIONSHIP_ENDPOINT_LABEL,
                schema,
                null,
                null,
                tokenNameLookup.labelGetName(endpointLabelId),
                endpointType,
                mask);
    }

    @Override
    public EndpointType endpointType() {
        return endpointType;
    }
}
