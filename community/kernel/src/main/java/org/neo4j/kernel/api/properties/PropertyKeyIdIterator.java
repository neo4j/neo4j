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
package org.neo4j.kernel.api.properties;

import org.eclipse.collections.api.iterator.IntIterator;

import java.util.Iterator;

import org.neo4j.storageengine.api.StorageProperty;

public class PropertyKeyIdIterator implements IntIterator
{
    private final Iterator<? extends StorageProperty> properties;

    public PropertyKeyIdIterator( Iterator<? extends StorageProperty> properties )
    {
        this.properties = properties;
    }

    @Override
    public boolean hasNext()
    {
        return properties.hasNext();
    }

    @Override
    public int next()
    {
        return properties.next().propertyKeyId();
    }
}
