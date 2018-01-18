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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;

public interface TokenRead
{
    /**
     * Value indicating the a token does not exist in the graph.
     */
    int NO_TOKEN = -1;

    /**
     * Returns the name of a label given its label id
     *
     * @param labelId The label id
     * @return The name of the label
     * @throws LabelNotFoundKernelException if no label is associates with this id
     */
    String nodeLabelName( int labelId ) throws LabelNotFoundKernelException;

    /**
     * Return the id of the provided label, or NO_TOKEN if the label isn't known to the graph.
     *
     * @param name The label name.
     * @return the label id, or NO_TOKEN
     */
    int nodeLabel( String name );

    /**
     * Return the id of the provided relationship type, or NO_TOKEN if the type isn't known to the graph.
     *
     * @param name The relationship type name.
     * @return the relationship type id, or NO_TOKEN
     */
    int relationshipType( String name );

    /**
     * Returns the name of a relationship type given its id
     *
     * @param relationshipTypeId The id of the relationship type
     * @return The name of the relationship type
     */
    String relationshipTypeName( int relationshipTypeId ) throws KernelException;

    /**
     * Return the id of the provided property key, or NO_TOKEN if the property isn't known to the graph.
     *
     * @param name The property key name.
     * @return the property key id, or NO_TOKEN
     */
    int propertyKey( String name );

    /**
     * Returns the name of a property given its property key id
     *
     * @param propertyKeyId The id of the property
     * @return The name of the key
     * @throws PropertyKeyIdNotFoundKernelException if no key is associated with the id
     */
    String propertyKeyName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;
}
