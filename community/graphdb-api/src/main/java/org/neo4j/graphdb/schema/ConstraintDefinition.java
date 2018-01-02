/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Definition of a constraint.
 *
 * <b>Note:</b> This interface is going to be changed/removed in next major release to better cope with node and
 * relationship constraints which are quite different concepts.
 */
public interface ConstraintDefinition
{
    /**
     * This accessor method returns a label which this constraint is associated with if this constraint has type
     * {@link ConstraintType#UNIQUENESS} or {@link ConstraintType#NODE_PROPERTY_EXISTENCE}.
     * Type of the constraint can be examined by calling {@link #getConstraintType()} or
     * {@link #isConstraintType(ConstraintType)} methods.
     *
     * @return the {@link Label} this constraint is associated with.
     * @throws IllegalStateException when this constraint is associated with relationships.
     */
    Label getLabel();

    /**
     * This accessor method returns a relationship type which this constraint is associated with if this constraint
     * has type {@link ConstraintType#UNIQUENESS} or {@link ConstraintType#NODE_PROPERTY_EXISTENCE}.
     * Type of the constraint can be examined by calling {@link #getConstraintType()} or
     * {@link #isConstraintType(ConstraintType)} methods.
     *
     * @return the {@link RelationshipType} this constraint is associated with.
     * @throws IllegalStateException when this constraint is associated with nodes.
     */
    RelationshipType getRelationshipType();

    /**
     * @return the property keys this constraint is about.
     */
    Iterable<String> getPropertyKeys();

    /**
     * Drops this constraint.
     */
    void drop();

    /**
     * @return the {@link ConstraintType} of constraint.
     */
    ConstraintType getConstraintType();

    /**
     * @param type a constraint type
     * @return true if this constraint definition's type is equal to the provided type
     */
    boolean isConstraintType( ConstraintType type );
}
