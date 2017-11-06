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

/**
 * Surface for getting schema information, such as fetching specific indexes or constraints.
 */
public interface SchemaRead
{
    /**
     * Acquire a reference to the index mapping the given {@code label} and {@code properties}.
     *
     * @param label the index label
     * @param properties the index properties
     * @return the IndexReference, or {@link CapableIndexReference#NO_INDEX} if such an index does not exist.
     */
    CapableIndexReference index( int label, int... properties );
}
