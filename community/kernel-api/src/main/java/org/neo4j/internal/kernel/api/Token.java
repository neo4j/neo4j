/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

/**
 * Token creation and lookup.
 */
public interface Token
{
    /**
     * Value indicating the a token does not exist in the graph.
     */
    int NO_TOKEN = -1;

    /**
     * Returns a label id for a label name. If the label doesn't exist prior to
     * this call it gets created.
     */
    int labelGetOrCreateForName( String labelName ) throws KernelException;

    /**
     * Returns a property key id for a property key. If the key doesn't exist prior to
     * this call it gets created.
     */
    int propertyKeyGetOrCreateForName( String propertyKeyName ) throws KernelException;

    int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws KernelException;

    void labelCreateForName( String labelName, int id ) throws KernelException;

    void propertyKeyCreateForName( String propertyKeyName, int id ) throws KernelException;

    void relationshipTypeCreateForName( String relationshipTypeName, int id ) throws KernelException;

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
     * Return the id of the provided property key, or NO_TOKEN if the property isn't known to the graph.
     *
     * @param name The property key name.
     * @return the property key id, or NO_TOKEN
     */
    int propertyKey( String name );
}
