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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * Cursor for all properties on a node or relationship.
 */
public class PropertyCursor extends AbstractPropertyCursor
{
    private final Consumer<PropertyCursor> consumer;

    private Iterator<StorageProperty> storagePropertyIterator;

    public PropertyCursor( PropertyStore propertyStore, Consumer<PropertyCursor> consumer )
    {
        super( propertyStore );
        this.consumer = consumer;
    }

    public PropertyCursor init( long firstPropertyId, Lock lock, PropertyContainerState state )
    {
        storagePropertyIterator = state.addedProperties();
        initialize( ALWAYS_TRUE_INT, firstPropertyId, lock, state );
        return this;
    }

    @Override
    protected boolean loadNextFromDisk()
    {
        return true;
    }

    @Override
    protected DefinedProperty nextAdded()
    {
        if ( storagePropertyIterator.hasNext() )
        {
            return (DefinedProperty) storagePropertyIterator.next();
        }
        return null;
    }

    @Override
    protected void doClose()
    {
        consumer.accept( this );
    }
}
