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
import org.neo4j.internal.schema.NodeLabelExistenceSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.string.Mask;

final class NodeLabelExistenceConstraintDescriptorImplementation extends ConstraintDescriptorAdaptor
        implements NodeLabelExistenceConstraintDescriptor {
    private final String name;
    private final NodeLabelExistenceSchemaDescriptor schema;
    private final int requiredLabelId;

    private NodeLabelExistenceConstraintDescriptorImplementation(
            NodeLabelExistenceSchemaDescriptor schema, long id, int requiredLabelId, String name) {
        super(id);
        if (requiredLabelId < 0) {
            throw new IllegalArgumentException("requiredLabelId cannot be negative");
        }
        this.schema = requireNonNull(schema, "NodeLabelExistenceSchemaDescriptor cannot be null");
        if (requiredLabelId == schema.getLabelId()) {
            throw new IllegalArgumentException("requiredLabelId cannot be same as schema labelId");
        }
        this.requiredLabelId = requiredLabelId;
        this.name = name;
    }

    static NodeLabelExistenceConstraintDescriptor make(NodeLabelExistenceSchemaDescriptor schema, int requiredLabelId) {
        return new NodeLabelExistenceConstraintDescriptorImplementation(schema, NO_ID, requiredLabelId, null);
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public ConstraintType type() {
        return ConstraintType.NODE_LABEL_EXISTENCE;
    }

    @Override
    public GraphTypeDependence graphTypeDependence() {
        return GraphTypeDependence.DEPENDENT;
    }

    @Override
    public boolean isNodeLabelExistenceConstraint() {
        return true;
    }

    @Override
    public NodeLabelExistenceConstraintDescriptor withId(long newId) {
        return new NodeLabelExistenceConstraintDescriptorImplementation(schema, newId, requiredLabelId, name);
    }

    @Override
    public NodeLabelExistenceConstraintDescriptor withName(String newName) {
        if (newName == null) {
            return this;
        }
        newName = SchemaNameUtil.sanitiseName(newName);
        return new NodeLabelExistenceConstraintDescriptorImplementation(schema, id, requiredLabelId, newName);
    }

    @Override
    public IndexBackedConstraintDescriptor withOwnedIndexId(long id) {
        throw new NotImplementedException();
    }

    @Override
    public NodeLabelExistenceConstraintDescriptor asNodeLabelExistenceConstraint() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NodeLabelExistenceConstraintDescriptor that)) {
            return false;
        }
        return equalsIgnoreName(that) && Objects.equals(this.name, that.getName());
    }

    @Override
    public boolean equalsIgnoreName(ConstraintDescriptor that) {
        // ugly, needed since equalsIgnoreName might be called from something else than equals
        if (!that.isNodeLabelExistenceConstraint()) {
            return false;
        }

        if (this.requiredLabelId != that.asNodeLabelExistenceConstraint().requiredLabelId()) {
            return false;
        }

        return this.schema().equals(that.schema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, requiredLabelId, name);
    }

    // It is not allowed to have a label be the constrained label for one constraint and required label for another
    // constraint
    @Override
    public boolean conflictsWith(ConstraintDescriptor other) {
        if (other.graphTypeDependence() == GraphTypeDependence.DEPENDENT) {
            if (other.isNodeLabelExistenceConstraint()) {
                NodeLabelExistenceConstraintDescriptor that = other.asNodeLabelExistenceConstraint();
                return this.schema().getLabelId() == that.requiredLabelId()
                        || this.requiredLabelId() == that.schema().getLabelId();
            } else if (other.isNodePropertyTypeConstraint() || other.isNodePropertyExistenceConstraint()) {
                return this.requiredLabelId() == other.schema().getLabelId();
            }
        }
        return false;
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
    public int schemaLabelId() {
        return schema.getLabelId();
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
                tokenNameLookup,
                id,
                name,
                ConstraintType.NODE_LABEL_EXISTENCE,
                schema,
                null,
                null,
                tokenNameLookup.labelGetName(requiredLabelId),
                null,
                mask);
    }

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return userDescription(tokenNameLookup, Mask.NO);
    }
}
