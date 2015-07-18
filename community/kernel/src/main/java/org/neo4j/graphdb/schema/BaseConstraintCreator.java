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

import org.neo4j.graphdb.ConstraintViolationException;

/**
 * Base interface for {@link ConstraintCreator} and {@link RelationshipConstraintCreator}, used to ensure that
 * the methods that are common share the same signature. But this interface is not exposed in the public API.
 */
interface BaseConstraintCreator<Creator extends BaseConstraintCreator<Creator>>
{
    Creator assertPropertyExists( String propertyKey );

    /**
     * Creates a constraint with the details specified by the other methods in this interface.
     *
     * @return the created {@link ConstraintDefinition constraint}.
     * @throws ConstraintViolationException if creating this constraint would violate any existing constraints.
     */
    ConstraintDefinition create() throws ConstraintViolationException;
}
