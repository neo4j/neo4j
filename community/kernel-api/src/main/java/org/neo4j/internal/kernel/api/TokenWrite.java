/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
     * Get or create the label token ids for each of the given {@code labelNames}, and store them at the corresponding
     * index in the given {@code labelIds} array.
     *
     * This is effectively a batching version of {@link #labelGetOrCreateForName(String)}.
     *
     * @param labelNames The array of label names for which to resolve or create their id.
     * @param labelIds The array into which the resulting token ids will be stored.
     * @throws TooManyLabelsException if too many labels would bve created by this call, compared to the token id space
     * available.
     */
    void labelGetOrCreateForNames( String[] labelNames, int[] labelIds )
            throws IllegalTokenNameException, TooManyLabelsException;

    /**
     * Creates a label with the given name.
     *
     * @param labelName the name of the label.
     * @return id of the created label.
     */
    int labelCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException;

    /**
     * Creates a property token with the given name.
     *
     * @param propertyKeyName the name of the property.
     * @return id of the created property key.
     */
    int propertyKeyCreateForName( String propertyKeyName ) throws IllegalTokenNameException;

    /**
     * Creates a relationship type with the given name.
     * @param relationshipTypeName the name of the relationship.
     * @return id of the created relationship type.
     */
    int relationshipTypeCreateForName( String relationshipTypeName ) throws IllegalTokenNameException;

    /**
     * Returns a property key id for a property key. If the key doesn't exist prior to
     * this call it gets created.
     */
    int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException;

    /**
     * Get or create the property token ids for each of the given {@code propertyKeys}, and store them at the
     * corresponding index in the given {@code ids} array.
     *
     * This is effectively a batching version of {@link #propertyKeyGetOrCreateForName(String)}.
     *
     * @param propertyKeys The array of property names for which to resolve or create their id.
     * @param ids The array into which the resulting token ids will be stored.
     */
    void propertyKeyGetOrCreateForNames( String[] propertyKeys, int[] ids ) throws IllegalTokenNameException;

    /**
     * Returns the id associated with the relationship type or creates a new one.
     * @param relationshipTypeName the name of the relationship
     * @return the id associated with the name
     */
    int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException;

    /**
     * Get or create the relationship type token ids for each of the given {@code relationshipTypes}, and store them at
     * the corresponding index in the given {@code ids} array.
     *
     * This is effectively a batching version of {@link #relationshipTypeGetOrCreateForName(String)}.
     *
     * @param relationshipTypes The array of relationship type names for which to resolve or create their id.
     * @param ids The array into which the resulting token ids will be stored.
     */
    void relationshipTypeGetOrCreateForNames( String[] relationshipTypes, int[] ids ) throws IllegalTokenNameException;
}
