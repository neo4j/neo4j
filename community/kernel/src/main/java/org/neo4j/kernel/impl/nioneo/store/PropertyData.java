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
package org.neo4j.kernel.impl.nioneo.store;

import org.neo4j.kernel.impl.cache.SizeOf;

public interface PropertyData extends SizeOf
{
    /**
     * @return the property record id.
     */
    long getId();
    
    /**
     * @return the property index
     */
    int getIndex();
    
    /**
     * @return the value of the property. This can be null if the value
     * hasn't been loaded yet.
     */
    Object getValue();

    /**
     * Sets the value of this {@link PropertyData} if it hasn't been set
     * before (light loading of the node). The instance containing the
     * new value will be returned. It can be the same or a new instance
     * depending on what would be more suitable for the {@code newValue}.
     * 
     * This method is only valid for String/Array property types. All other
     * get loaded directly.
     * 
     * @param newValue the new value to set.
     */
    void setNewValue( Object newValue );
}
