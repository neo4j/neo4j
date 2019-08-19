/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

public interface ConstraintDescriptor extends SchemaDescriptorSupplier, SchemaRule
{
    @Override
    SchemaDescriptor schema();

    ConstraintType type();

    boolean enforcesUniqueness();

    boolean enforcesPropertyExistence();

    String prettyPrint( TokenNameLookup tokenNameLookup );

    /**
     * Test if this constraint descriptor is a relationship property existence constraint.
     * @return {@code true} if calling {@link #asRelationshipPropertyExistenceConstraint()} would not throw.
     */
    boolean isRelationshipPropertyExistenceConstraint();

    /**
     * @return this constraint descriptor as a {@link RelExistenceConstraintDescriptor} if possible, or throw an {@link IllegalStateException}.
     */
    RelExistenceConstraintDescriptor asRelationshipPropertyExistenceConstraint();

    /**
     * Test if this constraint descriptor is a node property existence constraint.
     * @return {@code true} if calling {@link #asNodePropertyExistenceConstraint()} would not throw.
     */
    boolean isNodePropertyExistenceConstraint();

    /**
     * @return this constraint descriptor as a {@link NodeExistenceConstraintDescriptor} if possible, or throw an {@link IllegalStateException}.
     */
    NodeExistenceConstraintDescriptor asNodePropertyExistenceConstraint();

    /**
     * Test if this constraint descriptor is a uniqueness constraint.
     * @return {@code true} if calling {@link #asUniquenessConstraint()} would not throw.
     */
    boolean isUniquenessConstraint();

    /**
     * @return this constraint descriptor as a {@link UniquenessConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    UniquenessConstraintDescriptor asUniquenessConstraint();

    /**
     * Test if this constraint descriptor is a node key constraint.
     * @return {@code true} if calling {@link #asNodeKeyConstraint()} would not throw.
     */
    boolean isNodeKeyConstraint();

    /**
     * @return this constraint descriptor as a {@link NodeKeyConstraintDescriptor} if possible, or throw a {@link IllegalStateException}.
     */
    NodeKeyConstraintDescriptor asNodeKeyConstraint();

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
     * Produce a copy of this constraint descriptor, that has the given id.
     * @param id The id of the new constraint descriptor.
     * @return a modified copy of this constraint descriptor.
     */
    ConstraintDescriptor withId( long id );

    /**
     * Produce a copy of this constraint descriptor, that has the given name.
     * If the given name is {@code null}, then this descriptor is returned unchanged.
     * @param name The name of the new constraint descriptor.
     * @return a modified copy of this constraint descriptor.
     */
    @Override
    ConstraintDescriptor withName( String name );

    /**
     * Produce a copy of this constraint descriptor, that has the given owned index id.
     * @param id the id of the index that this constraint descriptor owns.
     * @return a modified copy of this constraint descriptor.
     */
    IndexBackedConstraintDescriptor withOwnedIndexId( long id );

    /**
     * Return the id of this constraint descriptor, if it has any, or throw an {@link IllegalStateException}.
     * @return the id of this constraint descriptor.
     * @see SchemaRule#getId()
     */
    long getId();
}
