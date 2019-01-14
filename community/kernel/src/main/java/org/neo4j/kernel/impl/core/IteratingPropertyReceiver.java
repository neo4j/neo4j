/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState.PropertyReceiver;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * A {@link PropertyReceiver} which can be iterated over once populated. The receiver and iterator is the
 * same object to save garbage.
 */
public class IteratingPropertyReceiver<P extends StorageProperty> extends PrefetchingIterator<P>
        implements PropertyReceiver<P>
{
    private StorageProperty[] properties = new StorageProperty[9];
    private int writeCursor;
    private int readCursor;

    @Override
    public void receive( P property, long propertyRecordId )
    {
        if ( writeCursor >= properties.length )
        {
            properties = Arrays.copyOf( properties, properties.length * 2 );
        }
        properties[writeCursor++] = property;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected P fetchNextOrNull()
    {
        if ( readCursor >= properties.length )
        {
            return null;
        }
        return (P) properties[readCursor++];
    }
}
