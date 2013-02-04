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
package org.neo4j.graphdb;

/**
 * Interface for managing the schema of your graph database. This currently includes
 * the new indexing support, added in Neo4j 2.0, please see the Neo4j manual for details.
 */
public interface Schema
{
    /**
     * Enable indexing of the specified property for nodes with the specified label.
     * All existing and all future nodes with the specified label will be indexed, speeding
     * up future operations to look up these nodes using the indexed property.
     *
     * @param label Label to tie index rule to.
     * @param propertyKey Property to index.
     */
    void createIndex( Label label, String propertyKey );

    /**
     * Retrieve a list of indexed property keys.
     * @param label
     * @return the property keys that have indexes for the given label.
     */
    Iterable<String> getIndexes( Label label );
}
