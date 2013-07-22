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

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;

/**
 * A builder for entering details about a constraint to create. After all details have been entered
 * {@link #create()} must be called for the constraint to actually be created. A constraint creator knows
 * which {@link Label label} it is to be created for.
 *
 * All methods except {@link #create()} will return an {@link ConstraintCreator} which should be
 * used for further interaction.
 *
 * @see Schema
 */
public interface ConstraintCreator
{
    /**
     * Specifies that this constraint will be for the given property key. Specifying a
     * property key requires further details to be given, such as {@link #unique() uniqueness}.
     *
     * @param propertyKey the property key this constraint is about.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator on( String propertyKey );

    /**
     * Imposes a uniqueness constraint for the {@link #on(String) given property}, such that
     * there can be at most one node, having the given label, for any set value of that property key.
     *
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator unique();

    /**
     * Creates a constraint with the details specified by the other methods in this interface.
     *
     * @return the created {@link ConstraintDefinition constraint}.
     * @throws ConstraintViolationException if creating this constraint would violate any
     * existing constraints.
     */
    ConstraintDefinition create() throws ConstraintViolationException;
}
