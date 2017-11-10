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
import org.neo4j.values.storable.Value;

/**
 * Operations for querying and seeking in explicit indexes.
 */
public interface ExplicitIndexRead
{
    void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Value value )
            throws KernelException;

    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query )
            throws KernelException;

    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws KernelException;

    void relationshipExplicitIndexGet(
            RelationshipExplicitIndexCursor cursor, String index, String key, Value value, long source, long target )
            throws KernelException;

    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, Object query, long source, long target )
            throws KernelException;

    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, String key, Object query, long source, long target )
            throws KernelException;
}
