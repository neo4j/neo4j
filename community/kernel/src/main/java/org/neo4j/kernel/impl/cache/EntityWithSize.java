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
package org.neo4j.kernel.impl.cache;

public interface EntityWithSize extends SizeOf
{
    /**
     * Id of this entity.
     * @return the id of this entity.
     */
    long getId();
    
    /**
     * Sets the size which was registered with the cache which this entity is in.
     * @param size the size to store for future retrieval in {@link #getRegisteredSize()}.
     */
    void setRegisteredSize( int size );
    
    /**
     * Returns the most recent registered size from {@link #setRegisteredSize(int)}.
     * Called from the cache that this entity is in.
     * @return the registered size of this entity.
     */
    int getRegisteredSize();
}
