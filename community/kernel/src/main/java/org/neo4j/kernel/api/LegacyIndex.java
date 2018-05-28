/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;

/**
 * The main way to access a legacy index. Even pure reads will need to get a hold of an object of this class
 * and to a query on. Blending of transaction state must also be handled within this object.
 */
public interface LegacyIndex
{
    LegacyIndexHits get( String key, Object value ) throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits query( String key, Object queryOrQueryObject ) throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits query( Object queryOrQueryObject ) throws LegacyIndexNotFoundKernelException;

    void addNode( long entity, String key, Object value ) throws LegacyIndexNotFoundKernelException;

    void remove( long entity, String key, Object value );

    void remove( long entity, String key );

    void remove( long entity );

    void drop();

    // Relationship-index-specific accessors
    LegacyIndexHits get( String key, Object value, long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits query( String key, Object queryOrQueryObject, long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits query( Object queryOrQueryObject, long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    void addRelationship( long entity, String key, Object value, long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    void removeRelationship( long entity, String key, Object value, long startNode, long endNode );

    void removeRelationship( long entity, String key, long startNode, long endNode );

    void removeRelationship( long entity, long startNode, long endNode );
}
