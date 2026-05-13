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
import static org.neo4j.internal.schema.GraphTypeDependence.DEPENDENT;
import static org.neo4j.internal.schema.GraphTypeDependence.INDEPENDENT;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.Objects;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.string.Mask;

/**
 * Internal representation of a property existence constraint, including the schema unit it targets (eg. label-property combination).
 */
public class ExistenceConstraintDescriptorImplementation extends ConstraintDescriptorAdaptor
        implements ExistenceConstraintDescriptor {
    private final GraphTypeDependence graphTypeDependence;
    private final SchemaDescriptor schema;
    private final String name;

    static ExistenceConstraintDescriptor makeExistsConstraint(SchemaDescriptor schema, boolean isDependent) {
        return new ExistenceConstraintDescriptorImplementation(
                isDependent ? DEPENDENT : INDEPENDENT, schema, NO_ID, null);
    }

    private ExistenceConstraintDescriptorImplementation(
            GraphTypeDependence dependence, SchemaDescriptor schema, long id, String name) {
        super(id);
        this.graphTypeDependence = dependence;
        this.schema = schema;
        this.name = name;
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public ConstraintType type() {
        return ConstraintType.EXISTS;
    }

    @Override
    public GraphTypeDependence graphTypeDependence() {
        return graphTypeDependence;
    }

    @Override
    public boolean enforcesPropertyExistence() {
        return true;
    }

    @Override
    public boolean isPropertyExistenceConstraint() {
        return true;
    }

    @Override
    public boolean isRelationshipPropertyExistenceConstraint() {
        return schema.entityType() == RELATIONSHIP;
    }

    @Override
    public boolean isNodePropertyExistenceConstraint() {
        return schema.entityType() == NODE;
    }

    @Override
    public ExistenceConstraintDescriptor asPropertyExistenceConstraint() {
        return this;
    }

    @Override
    public ExistenceConstraintDescriptor withId(long id) {
        return new ExistenceConstraintDescriptorImplementation(graphTypeDependence, schema, id, name);
    }

    @Override
    public ExistenceConstraintDescriptor withName(String name) {
        if (name == null) {
            return this;
        }
        name = SchemaNameUtil.sanitiseName(name);
        return new ExistenceConstraintDescriptorImplementation(graphTypeDependence, schema, id, name);
    }

    @Override
    public IndexBackedConstraintDescriptor withOwnedIndexId(long id) {
        throw new IllegalStateException("ConstraintDescriptor missing IndexType when connected to index");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean conflictsWith(ConstraintDescriptor other) {
        if (graphTypeDependence == DEPENDENT
                && schema.entityType() == NODE
                && other instanceof NodeLabelExistenceConstraintDescriptorImplementation) {
            NodeLabelExistenceConstraintDescriptor that = other.asNodeLabelExistenceConstraint();
            if (this.schema.getLabelId() == that.requiredLabelId()) {
                return true;
            }
        }

        return other.type() == ConstraintType.EXISTS && this.schema.equals(other.schema());
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof ConstraintDescriptor that)) {
            return false;
        }
        return equalsIgnoreName(that) && Objects.equals(this.name, that.getName());
    }

    @Override
    public boolean equalsIgnoreName(ConstraintDescriptor that) {
        if (that.type() != EXISTS) {
            return false;
        }

        if (this.graphTypeDependence != that.graphTypeDependence()) {
            return false;
        }

        if (!this.schema().equals(that.schema())) {
            return false;
        }

        return !that.isIndexBackedConstraint();
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
    public final int hashCode() {
        return Objects.hash(EXISTS, graphTypeDependence, schema, name);
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
                tokenNameLookup, id, name, EXISTS, schema(), null, null, null, null, mask);
    }
}
