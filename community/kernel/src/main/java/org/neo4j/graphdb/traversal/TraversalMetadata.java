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
package org.neo4j.graphdb.traversal;

/**
 * Provides metadata about a traversal.
 * 
 * @author Mattias Persson
 */
public interface TraversalMetadata
{
    /**
     * @return number of paths returned up to this point in the traversal.
     */
    int getNumberOfPathsReturned();
    
    /**
     * @return number of relationships traversed up to this point in the traversal.
     * Some relationships in this counter might be unnecessarily traversed relationships,
     * but at the same time it gives an accurate measure of how many relationships are
     * requested from the underlying graph. Useful for comparing and first-level debugging
     * of queries.
     */
    int getNumberOfRelationshipsTraversed();
}
