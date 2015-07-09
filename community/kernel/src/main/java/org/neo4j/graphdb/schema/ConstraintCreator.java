/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * A builder for entering details about a constraint to create. After all details have been entered
 * {@link #create()} must be called for the constraint to actually be created. A constraint creator knows
 * which {@link org.neo4j.graphdb.Label label} it is to be created for.
 *
 * All methods except {@link #create()} will return an {@link ConstraintCreator} which should be
 * used for further interaction.
 *
 * Compatibility note: New methods may be added to this interface without notice,
 * backwards compatibility is only guaranteed for clients of this interface, not for
 * implementors.
 *
 * <b>NOTE:</b> this interface should in fact be named NodeConstraintCreator. Renaming is not done due to backwards
 * compatibility reasons will happen in the next major release.
 *
 * @see Schema
 */
public interface ConstraintCreator extends BaseConstraintCreator<ConstraintCreator>
{
    /**
     * Imposes a uniqueness constraint for the given property, such that
     * there can be at most one node, having the given label, for any set value of that property key.
     *
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator assertPropertyIsUnique( String propertyKey );

    /**
     * Imposes an existence constraint for the given property, such that any node with the given label must have a
     * value set for the given property key.
     *
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    @Override
    ConstraintCreator assertPropertyExists( String propertyKey );
}
