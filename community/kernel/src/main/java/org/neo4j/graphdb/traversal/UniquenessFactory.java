/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
 * Factory for {@link UniquenessFilter} filters, it can supply the
 * created {@link UniquenessFilter} with an optional parameter which is
 * up to the filter itself to validate and make sense of.
 */
public interface UniquenessFactory
{
    /**
     * Creates a new {@link UniquenessFilter} optionally with a parameter
     * to it, otherwise null if no parameter should be handed to it.
     * 
     * @param optionalParameter an optional parameter to control the behavior
     * of the returned {@link UniquenessFilter}. It's up to each filter implementation
     * to decide what values are OK and what they mean and the caller of this
     * method need to know that and pass in the correct parameter type.
     * @return a new {@link UniquenessFilter} of the type that this factory creates.
     */
    UniquenessFilter create( Object optionalParameter );
}
