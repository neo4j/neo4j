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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;

public interface TokenWrite
{
    /**
     * Returns a label id for a label name. If the label doesn't exist prior to
     * this call it gets created.
     */
    int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException;

    /**
     * Creates a label with the given id
     * @param labelName the name of the label
     * @param id the id of the label
     */
    void labelCreateForName( String labelName, int id ) throws IllegalTokenNameException, TooManyLabelsException;

    /**
     * Creates a property token with the given id
     * @param propertyKeyName the name of the property
     * @param id the id of the property
     */
    void propertyKeyCreateForName( String propertyKeyName, int id ) throws IllegalTokenNameException;

    /**
     * Creates a relationship type with the given id
     * @param relationshipTypeName the name of the relationship
     * @param id the relationship type
     */
    void relationshipTypeCreateForName( String relationshipTypeName, int id ) throws IllegalTokenNameException;

    /**
     * Returns a property key id for a property key. If the key doesn't exist prior to
     * this call it gets created.
     */
    int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException;

    /**
     * Returns the id associated with the relationship type or creates a new one.
     * @param relationshipTypeName the name of the relationship
     * @return the id associated with the name
     */
    int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException;
}
