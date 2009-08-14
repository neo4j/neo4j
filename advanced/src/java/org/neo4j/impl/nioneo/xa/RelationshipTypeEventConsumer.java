/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.impl.nioneo.store.RelationshipTypeData;

public interface RelationshipTypeEventConsumer
{
    /**
     * Adds a new relationship type.
     * 
     * @param id
     *            The id of the relationship type
     * @param name
     *            The name of the relationship type
     * @throws IOException
     *             If unable to add the relationship type
     */
    public void addRelationshipType( int id, String name );

    /**
     * Gets a relationship type with a given id.
     * 
     * @param id
     *            The id of the relationship type
     * @return The relationship type data
     * @throws IOException
     *             If unable to get relationship type
     */
    public RelationshipTypeData getRelationshipType( int id );

    /**
     * Gets all relationship types.
     * 
     * @return An array containing the relationship type data
     * @throws IOException
     *             If unable to get the relationship types
     */
    public RelationshipTypeData[] getRelationshipTypes();
}