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

import org.neo4j.internal.schema.constraints.ExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

public interface ConstraintDescriptor extends SchemaDescriptorSupplier, SchemaRule {
    int NO_ID = -1;

    @Override
    SchemaDescriptor schema();

    ConstraintType type();

    GraphTypeDependence graphTypeDependence();

    boolean enforcesUniqueness();

    boolean enforcesPropertyExistence();

    boolean enforcesPropertyType();

    /**
     * Test if this constraint descriptor is a property type constraint.
     * @return {@code true} if calling {@link #asPropertyTypeConstraint()} would not throw.
     */
    boolean isPropertyTypeConstraint();

    /**
     * Test if this constraint descriptor is a endpoint constraint.
     * @return {@code true} if calling {@link #asPropertyTypeConstraint()} would not throw.
     */
    boolean isRelationshipEndpointConstraint();

    /**
     * Test if this constraint descriptor is a label coexistence constraint.
     */
    boolean isLabelCoexistenceConstraint();

    /**
     * Test if this constraint descriptor is a node property type constraint.
     */
    boolean isNodePropertyTypeConstraint();

    /**
     * Test if this constraint descriptor is a relationship property type constraint.
     */
    boolean isRelationshipPropertyTypeConstraint();

    /**
     * @return this constraint descriptor as a {@link TypeConstraintDescriptor} if possible, or throw an {@link IllegalStateException}.
     */
    TypeConstraintDescriptor asPropertyTypeConstraint();

    /**
     * Test if this constraint descriptor is a property existence constraint.
     * @return {@code true} if calling {@link #asPropertyExistenceConstraint()} would not throw.
     */
    boolean isPropertyExistenceConstraint();

    /**
     * Test if this constraint descriptor is a relationship property existence constraint.
     */
    boolean isRelationshipPropertyExistenceConstraint();

    /**
     * Test if this constraint descriptor is a node property existence constraint.
     */
    boolean isNodePropertyExistenceConstraint();

    /**
     * @return this constraint descriptor as a {@link ExistenceConstraintDescriptor} if possible, or throw an {@link IllegalStateException}.
     */
    ExistenceConstraintDescriptor asPropertyExistenceConstraint();

    /**
     * Test if this constraint descriptor is a uniqueness constraint.
     * @return {@code true} if calling {@link #asUniquenessConstraint()} would not throw.
     */
    boolean isUniquenessConstraint();

    /**
     * Test if this constraint descriptor is a node uniqueness constraint.
     */
    boolean isNodeUniquenessConstraint();

    /**
     * Test if this constraint descriptor is a relationship uniqueness constraint.
     */
    boolean isRelationshipUniquenessConstraint();

    /**
     * @return this constraint descriptor as a {@link UniquenessConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    UniquenessConstraintDescriptor asUniquenessConstraint();

    /**
     * Test if this constraint descriptor is a node key constraint.
     */
    boolean isNodeKeyConstraint();

    /**
     * Test if this constraint descriptor is a relationship key constraint.
     */
    boolean isRelationshipKeyConstraint();

    /**
     * Test if this constraint descriptor is an index backed constraint descriptor.
     * @return {@code true} if calling {@link #asIndexBackedConstraint()} would not throw.
     */
    boolean isIndexBackedConstraint();

    /**
     * @return this constraint descriptor as an {@link IndexBackedConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    IndexBackedConstraintDescriptor asIndexBackedConstraint();

    /**
     * Test if this constraint descriptor is a key constraint descriptor.
     * @return {@code true} if calling {@link #asKeyConstraint()} would not throw.
     */
    boolean isKeyConstraint();

    /**
     * @return this constraint descriptor as an {@link KeyConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    KeyConstraintDescriptor asKeyConstraint();

    /**
     * @return this constraint descriptor as an {@link RelationshipEndpointConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    RelationshipEndpointConstraintDescriptor asRelationshipEndpointConstraint();

    /**
     * @return this constraint descriptor as an {@link LabelCoexistenceConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    LabelCoexistenceConstraintDescriptor asLabelCoexistenceConstraint();

    /**
     * Produce a copy of this constraint descriptor, that has the given id.
     * @param id The id of the new constraint descriptor.
     * @return a modified copy of this constraint descriptor.
     */
    ConstraintDescriptor withId(long id);

    /**
     * Produce a copy of this constraint descriptor, that has the given name.
     * If the given name is {@code null}, then this descriptor is returned unchanged.
     * @param name The name of the new constraint descriptor.
     * @return a modified copy of this constraint descriptor.
     */
    @Override
    ConstraintDescriptor withName(String name);

    /**
     * Produce a copy of this constraint descriptor, that has the given owned index id.
     * @param id the id of the index that this constraint descriptor owns.
     * @return a modified copy of this constraint descriptor.
     */
    IndexBackedConstraintDescriptor withOwnedIndexId(long id);

    /**
     * Return the id of this constraint descriptor, if it has any, or throw an {@link IllegalStateException}.
     * @return the id of this constraint descriptor.
     * @see SchemaRule#getId()
     */
    @Override
    long getId();
}
