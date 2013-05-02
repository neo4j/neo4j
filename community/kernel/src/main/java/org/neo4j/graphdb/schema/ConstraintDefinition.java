/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

/**
 * Definition of a constraint.
 */
public interface ConstraintDefinition
{
    /**
     * @return the {@link Label} this constraint is associated with.
     */
    Label getLabel();
    
    /**
     * Drops this constraint.
     */
    void drop();
    
    /**
     * @return access to details about uniqueness.
     * @throws IllegalStateException if this constraint is of an invalid type.
     */
    UniquenessConstraintDefinition asUniquenessConstraint();
    
    /**
     * @return the {@link Type} of constraint.
     */
    Type getConstraintType();

    /**
     * Constraint type. Used with {@link ConstraintDefinition#getConstraintType()} to see
     * of which type a constraint is.
     */
    public static enum Type
    {
        /**
         * @see ConstraintDefinition#asUniquenessConstraint()
         */
        UNIQUENESS;
    }
}
